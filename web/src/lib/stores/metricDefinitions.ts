/**
 * Metric Definitions Store
 * 
 * Manages caching and retrieval of metric definitions from the meta-data API
 * Provides efficient caching to avoid repeated API calls
 */

import { writable } from 'svelte/store';

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

interface RelatedMetric {
    related_metric_id: string;
    related_metric_name: string;
    relationship_type: string;
    relationship_description?: string;
    strength: number;
}

interface MetricData {
    metric: MetricDefinition;
    related_metrics?: RelatedMetric[];
    cached_at: number;
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

interface MetricDefinitionsState {
    metrics: Record<string, MetricData>;
    categories: MetricCategory[];
    loading: Record<string, boolean>;
    lastCategoriesFetch: number | null;
}

// Cache duration in milliseconds (15 minutes)
const CACHE_DURATION = 15 * 60 * 1000;

// Initial state
const initialState: MetricDefinitionsState = {
    metrics: {},
    categories: [],
    loading: {},
    lastCategoriesFetch: null
};

// Create the writable store
const { subscribe, set, update } = writable<MetricDefinitionsState>(initialState);

// Helper function to check if cached data is still valid
function isCacheValid(timestamp: number): boolean {
    return Date.now() - timestamp < CACHE_DURATION;
}

// Store interface with methods
export const metricDefinitionsStore = {
    subscribe,

    /**
     * Get a cached metric definition if available and valid
     */
    getMetric(metricId: string): MetricData | null {
        let cachedData: MetricData | null = null;
        
        update(state => {
            const cached = state.metrics[metricId];
            if (cached && isCacheValid(cached.cached_at)) {
                cachedData = cached;
            }
            return state;
        });
        
        return cachedData;
    },

    /**
     * Cache a metric definition
     */
    setMetric(metricId: string, data: { metric: MetricDefinition; related_metrics?: RelatedMetric[] }): void {
        update(state => ({
            ...state,
            metrics: {
                ...state.metrics,
                [metricId]: {
                    ...data,
                    cached_at: Date.now()
                }
            },
            loading: {
                ...state.loading,
                [metricId]: false
            }
        }));
    },

    /**
     * Set loading state for a metric
     */
    setLoading(metricId: string, loading: boolean): void {
        update(state => ({
            ...state,
            loading: {
                ...state.loading,
                [metricId]: loading
            }
        }));
    },

    /**
     * Get cached categories if available and valid
     */
    getCategories(): MetricCategory[] | null {
        let cachedCategories: MetricCategory[] | null = null;
        
        update(state => {
            if (state.lastCategoriesFetch && isCacheValid(state.lastCategoriesFetch)) {
                cachedCategories = state.categories;
            }
            return state;
        });
        
        return cachedCategories;
    },

    /**
     * Cache categories
     */
    setCategories(categories: MetricCategory[]): void {
        update(state => ({
            ...state,
            categories,
            lastCategoriesFetch: Date.now()
        }));
    },

    /**
     * Fetch and cache a specific metric definition
     */
    async fetchMetric(metricId: string, includeRelated: boolean = true): Promise<MetricData> {
        // Check cache first
        const cached = this.getMetric(metricId);
        if (cached) {
            return cached;
        }

        // Set loading state
        this.setLoading(metricId, true);

        try {
            const response = await fetch(`/api/meta-data?metric_id=${encodeURIComponent(metricId)}&include_related=${includeRelated}`);
            
            if (!response.ok) {
                throw new Error(`Failed to fetch metric definition: ${response.status}`);
            }

            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }

            // Cache the result
            this.setMetric(metricId, data);

            return {
                metric: data.metric,
                related_metrics: data.related_metrics || [],
                cached_at: Date.now()
            };

        } catch (error) {
            this.setLoading(metricId, false);
            throw error;
        }
    },

    /**
     * Fetch and cache all categories
     */
    async fetchCategories(): Promise<MetricCategory[]> {
        // Check cache first
        const cached = this.getCategories();
        if (cached) {
            return cached;
        }

        try {
            const response = await fetch('/api/meta-data?include_categories=true');
            
            if (!response.ok) {
                throw new Error(`Failed to fetch categories: ${response.status}`);
            }

            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }

            const categories = data.categories || [];
            this.setCategories(categories);

            return categories;

        } catch (error) {
            console.error('Error fetching categories:', error);
            throw error;
        }
    },

    /**
     * Fetch metrics by category
     */
    async fetchMetricsByCategory(categoryId: string): Promise<MetricDefinition[]> {
        try {
            const response = await fetch(`/api/meta-data?category=${encodeURIComponent(categoryId)}`);
            
            if (!response.ok) {
                throw new Error(`Failed to fetch metrics for category: ${response.status}`);
            }

            const data = await response.json();
            
            if (data.error) {
                throw new Error(data.error);
            }

            // Cache individual metrics
            data.metrics?.forEach((metric: MetricDefinition) => {
                this.setMetric(metric.metric_id, { metric });
            });

            return data.metrics || [];

        } catch (error) {
            console.error('Error fetching metrics by category:', error);
            throw error;
        }
    },

    /**
     * Clear all cached data
     */
    clearCache(): void {
        set(initialState);
    },

    /**
     * Clear expired cache entries
     */
    clearExpiredCache(): void {
        update(state => {
            const now = Date.now();
            const validMetrics: Record<string, MetricData> = {};

            // Keep only valid cached metrics
            Object.entries(state.metrics).forEach(([id, data]) => {
                if (isCacheValid(data.cached_at)) {
                    validMetrics[id] = data;
                }
            });

            return {
                ...state,
                metrics: validMetrics,
                lastCategoriesFetch: 
                    state.lastCategoriesFetch && isCacheValid(state.lastCategoriesFetch) 
                        ? state.lastCategoriesFetch 
                        : null,
                categories: 
                    state.lastCategoriesFetch && isCacheValid(state.lastCategoriesFetch)
                        ? state.categories
                        : []
            };
        });
    },

    /**
     * Preload commonly used metrics
     */
    async preloadCommonMetrics(): Promise<void> {
        const commonMetrics = [
            'hall_of_fame_index',
            'career_win_percentage',
            'competitiveness_index',
            'faab_efficiency_rating',
            'season_consistency_score'
        ];

        const promises = commonMetrics.map(metricId => 
            this.fetchMetric(metricId, false).catch(err => {
                console.warn(`Failed to preload metric ${metricId}:`, err);
                return null;
            })
        );

        await Promise.all(promises);
    },

    /**
     * Get cache statistics for debugging
     */
    getCacheStats(): { totalMetrics: number; validMetrics: number; categories: number } {
        let stats = { totalMetrics: 0, validMetrics: 0, categories: 0 };
        
        update(state => {
            const now = Date.now();
            stats.totalMetrics = Object.keys(state.metrics).length;
            stats.validMetrics = Object.values(state.metrics).filter(data => 
                isCacheValid(data.cached_at)
            ).length;
            stats.categories = state.lastCategoriesFetch && isCacheValid(state.lastCategoriesFetch) 
                ? state.categories.length 
                : 0;
            return state;
        });
        
        return stats;
    }
};

// Export the store for use in components (both named and default for compatibility)
export default metricDefinitionsStore;

// Auto-cleanup expired cache every 5 minutes
if (typeof window !== 'undefined') {
    setInterval(() => {
        metricDefinitionsStore.clearExpiredCache();
    }, 5 * 60 * 1000);
} 