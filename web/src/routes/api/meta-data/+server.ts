import { json } from '@sveltejs/kit';
import type { RequestHandler } from './$types';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';

/**
 * Meta-data API endpoint for serving metric definitions and data dictionary
 * 
 * Supports queries for:
 * - Individual metric definitions
 * - Category listings
 * - Related metrics
 * - Field definitions for API endpoints
 */

interface MetricDefinition {
    metric_id: string;
    metric_name: string;
    metric_category: string;
    category_name?: string;
    short_description: string;
    detailed_description?: string;
    calculation_formula?: string;
    example_calculation?: string;
    interpretation_guide?: string;
    data_type: string;
    unit_of_measure?: string;
    typical_range?: string;
    good_value_threshold?: number;
    excellent_value_threshold?: number;
    display_format: string;
    sort_order: number;
    category_order?: number;
    category_icon?: string;
    category_color?: string;
}

interface MetricCategory {
    category_id: string;
    category_name: string;
    category_description?: string;
    display_order: number;
    icon_name?: string;
    color_scheme?: string;
    metric_count?: number;
}

interface RelatedMetric {
    related_metric_id: string;
    related_metric_name: string;
    relationship_type: string;
    relationship_description?: string;
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
        if (include_categories || (!metric_id && !category && !endpoint)) {
            const categoriesQuery = `
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
            `;
            
            const categoriesResult = await db.execute(sql.raw(categoriesQuery));
            const categories = Array.from(categoriesResult) as unknown as MetricCategory[];
            
            if (include_categories && !metric_id && !category && !endpoint) {
                return json({ categories });
            }
        }

        // If requesting specific metric definition
        if (metric_id) {
            // Escape single quotes to prevent SQL injection
            const escapedMetricId = metric_id.replace(/'/g, "''");
            const metricQuery = `
                SELECT * FROM meta_data.vw_active_metrics 
                WHERE metric_id = '${escapedMetricId}'
            `;
            
            const metricResult = await db.execute(sql.raw(metricQuery));
            const metricArray = Array.from(metricResult) as unknown as MetricDefinition[];
            const metric = metricArray[0];
            
            if (!metric) {
                return json({ error: 'Metric not found' }, { status: 404 });
            }

            let relatedMetrics: RelatedMetric[] = [];
            if (include_related) {
                const relatedQuery = `
                    SELECT 
                        related_metric_id,
                        related_metric_name,
                        relationship_type,
                        relationship_description,
                        strength
                    FROM meta_data.vw_metric_relationships 
                    WHERE primary_metric_id = '${escapedMetricId}'
                    ORDER BY strength DESC, related_metric_name
                `;
                
                const relatedResult = await db.execute(sql.raw(relatedQuery));
                relatedMetrics = Array.from(relatedResult) as unknown as RelatedMetric[];
            }

            return json({
                metric,
                related_metrics: relatedMetrics
            });
        }

        // If requesting metrics by category
        if (category) {
            const escapedCategory = category.replace(/'/g, "''");
            const metricsQuery = `
                SELECT * FROM meta_data.vw_active_metrics 
                WHERE metric_category = '${escapedCategory}'
                ORDER BY sort_order, metric_name
            `;
            
            const metricsResult = await db.execute(sql.raw(metricsQuery));
            const metrics = Array.from(metricsResult) as unknown as MetricDefinition[];
            
            return json({ metrics });
        }

        // If requesting field definitions for an API endpoint
        if (endpoint) {
            const escapedEndpoint = endpoint.replace(/'/g, "''");
            const fieldsQuery = `
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
                WHERE fd.api_endpoint = '${escapedEndpoint}'
                ORDER BY fd.field_name
            `;
            
            const fieldsResult = await db.execute(sql.raw(fieldsQuery));
            const fields = Array.from(fieldsResult);
            
            return json({ endpoint, fields });
        }

        // Default: return all active metrics grouped by category
        const allMetricsQuery = `
            SELECT * FROM meta_data.vw_active_metrics 
            ORDER BY category_order, sort_order, metric_name
        `;
        
        const allMetricsResult = await db.execute(sql.raw(allMetricsQuery));
        const allMetrics = Array.from(allMetricsResult) as unknown as MetricDefinition[];
        
        // Group by category
        const groupedMetrics = allMetrics.reduce((acc, metric) => {
            const category = metric.metric_category;
            if (!acc[category]) {
                acc[category] = {
                    category_id: category,
                    category_name: metric.category_name || category,
                    category_icon: metric.category_icon,
                    category_color: metric.category_color,
                    metrics: []
                };
            }
            acc[category].metrics.push(metric);
            return acc;
        }, {} as Record<string, any>);

        return json({
            categories: Object.values(groupedMetrics)
        });

    } catch (error) {
        console.error('Meta-data API error:', error);
        return json(
            { error: 'Failed to fetch meta-data' },
            { status: 500 }
        );
    }
}; 