import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import { groupByCategory, toMetric, type MetricRow } from '$lib/server/meta-data/normalize';

/**
 * Meta-data API endpoint for serving metric definitions and data dictionary
 *
 * Supports queries for:
 * - Individual metric definitions
 * - Category listings
 * - Related metrics
 * - Field definitions for API endpoints
 *
 * Everything here speaks camelCase, matching the rest of the app's APIs and the
 * shape the DB client's `transform: postgres.camel` already produces. Rows go
 * through the mappers in $lib/server/meta-data/normalize rather than being
 * returned raw, so the contract is explicit rather than implied by `SELECT *`.
 */

interface MetricCategory {
	categoryId: string;
	categoryName: string;
	categoryDescription?: string;
	displayOrder: number;
	iconName?: string;
	colorScheme?: string;
	metricCount?: number;
}

interface RelatedMetric {
	relatedMetricId: string;
	relatedMetricName: string;
	relationshipType: string;
	relationshipDescription?: string;
	strength: number;
}

export const GET: RequestHandler = async ({ url }) => {
	try {
		const metric_id = url.searchParams.get('metric_id');
		const category = url.searchParams.get('category');
		const endpoint = url.searchParams.get('endpoint');
		const include_related = url.searchParams.get('include_related') === 'true';
		const include_categories = url.searchParams.get('include_categories') === 'true';

		// If requesting categories
		if (include_categories && !metric_id && !category && !endpoint) {
			const categoriesResult = await db.execute(sql`
                SELECT
                    mc.category_id,
                    mc.category_name,
                    mc.category_description,
                    mc.display_order,
                    mc.icon_name,
                    mc.color_scheme,
                    COUNT(md.metric_id) as metric_count
                FROM meta_data.metric_categories mc
                LEFT JOIN meta_data.metric_definitions md
                    ON mc.category_id = md.metric_category
                    AND md.is_active = true
                WHERE mc.is_active = true
                GROUP BY mc.category_id, mc.category_name, mc.category_description,
                         mc.display_order, mc.icon_name, mc.color_scheme
                ORDER BY mc.display_order, mc.category_name
            `);
			const categories = Array.from(categoriesResult) as unknown as MetricCategory[];

			return json({ categories });
		}

		// If requesting specific metric definition
		if (metric_id) {
			const metricResult = await db.execute(sql`
                SELECT * FROM meta_data.vw_active_metrics
                WHERE metric_id = ${metric_id}
            `);
			const metricArray = Array.from(metricResult) as unknown as MetricRow[];
			const row = metricArray[0];

			if (!row) {
				return json({ error: 'Metric not found' }, { status: 404 });
			}

			let relatedMetrics: RelatedMetric[] = [];
			if (include_related) {
				const relatedResult = await db.execute(sql`
                    SELECT
                        related_metric_id,
                        related_metric_name,
                        relationship_type,
                        relationship_description,
                        strength
                    FROM meta_data.vw_metric_relationships
                    WHERE primary_metric_id = ${metric_id}
                    ORDER BY strength DESC, related_metric_name
                `);
				relatedMetrics = Array.from(relatedResult) as unknown as RelatedMetric[];
			}

			return json({
				metric: toMetric(row),
				related_metrics: relatedMetrics
			});
		}

		// If requesting metrics by category
		if (category) {
			const metricsResult = await db.execute(sql`
                SELECT * FROM meta_data.vw_active_metrics
                WHERE metric_category = ${category}
                ORDER BY sort_order, metric_name
            `);
			const metrics = Array.from(metricsResult) as unknown as MetricRow[];

			return json({ metrics: metrics.map(toMetric) });
		}

		// If requesting field definitions for an API endpoint
		if (endpoint) {
			const fieldsResult = await db.execute(sql`
                SELECT
                    fd.field_name,
                    fd.field_description,
                    fd.is_required,
                    fd.default_value,
                    md.metric_id,
                    md.metric_name,
                    md.short_description,
                    md.data_type,
                    md.unit_of_measure,
                    md.display_format
                FROM meta_data.field_definitions fd
                LEFT JOIN meta_data.metric_definitions md ON fd.metric_id = md.metric_id
                WHERE fd.api_endpoint = ${endpoint}
                ORDER BY fd.field_name
            `);
			const fields = Array.from(fieldsResult);

			return json({ endpoint, fields });
		}

		// Default: return all active metrics grouped by category
		const allMetricsResult = await db.execute(sql`
            SELECT * FROM meta_data.vw_active_metrics
            ORDER BY category_order, sort_order, metric_name
        `);
		const allMetrics = Array.from(allMetricsResult) as unknown as MetricRow[];

		return json({
			categories: groupByCategory(allMetrics)
		});
	} catch (error) {
		console.error('Meta-data API error:', error);
		return json({ error: 'Failed to fetch meta-data' }, { status: 500 });
	}
};
