# Data Dictionary & Tooltip System Usage Guide

## Overview

The data dictionary system provides contextual help for all fantasy football metrics throughout the application. Users can hover over any metric to see detailed definitions, calculations, interpretation guides, and related metrics.

## Quick Start

### 1. Simple Tooltip with Help Icon

```svelte
<script>
  import MetricHelp from '$lib/components/MetricHelp.svelte';
</script>

<!-- Adds tooltip with help icon -->
<MetricHelp metricId="hall_of_fame_index">Hall of Fame Index</MetricHelp>
```

### 2. Tooltip Without Help Icon

```svelte
<script>
  import MetricTooltip from '$lib/components/MetricTooltip.svelte';
</script>

<!-- Just the text with tooltip on hover -->
<MetricTooltip metricId="career_win_percentage">
  <span class="font-semibold cursor-help">Win Percentage</span>
</MetricTooltip>
```

### 3. Icon-Only Help Button

```svelte
<MetricHelp metricId="competitiveness_index" iconOnly={true} />
```

## Component Props

### MetricHelp Component

| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `metricId` | string | required | The metric identifier (matches API field names) |
| `position` | string | 'top' | Tooltip position: 'top', 'bottom', 'left', 'right' |
| `includeRelated` | boolean | true | Show related metrics in tooltip |
| `maxWidth` | string | '400px' | Maximum width of tooltip |
| `theme` | string | 'light' | Theme: 'light' or 'dark' |
| `showIcon` | boolean | true | Show help icon |
| `iconOnly` | boolean | false | Show only icon, no text |
| `className` | string | '' | Additional CSS classes |

### MetricTooltip Component

| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `metricId` | string | required | The metric identifier |
| `position` | string | 'top' | Tooltip position |
| `includeRelated` | boolean | true | Show related metrics |
| `maxWidth` | string | '400px' | Maximum tooltip width |
| `theme` | string | 'light' | Color theme |

## Real-World Integration Examples

### Example 1: Hall of Fame Page

```svelte
<!-- Before -->
<td class="px-6 py-4 text-sm text-gray-900">
  {manager.hall_of_fame_index.toFixed(3)}
</td>

<!-- After -->
<td class="px-6 py-4 text-sm text-gray-900">
  <MetricTooltip metricId="hall_of_fame_index" position="bottom">
    <span class="cursor-help hover:text-blue-600 transition-colors">
      {manager.hall_of_fame_index.toFixed(3)}
    </span>
  </MetricTooltip>
</td>
```

### Example 2: Standings Table Header

```svelte
<!-- Before -->
<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
  Win %
</th>

<!-- After -->
<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
  <MetricHelp metricId="career_win_percentage" position="bottom">
    Win %
  </MetricHelp>
</th>
```

### Example 3: Card Metrics

```svelte
<!-- Before -->
<div class="text-center">
  <div class="text-2xl font-bold text-gray-900">{competitiveness.toFixed(1)}</div>
  <div class="text-sm text-gray-500">Competitiveness Index</div>
</div>

<!-- After -->
<div class="text-center">
  <MetricTooltip metricId="competitiveness_index" position="top">
    <div class="cursor-help">
      <div class="text-2xl font-bold text-gray-900 hover:text-blue-600 transition-colors">
        {competitiveness.toFixed(1)}
      </div>
      <div class="text-sm text-gray-500">Competitiveness Index</div>
    </div>
  </MetricTooltip>
</div>
```

### Example 4: Stats Grid

```svelte
<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
  <div class="bg-white p-4 rounded-lg border">
    <MetricHelp metricId="faab_efficiency_rating" position="top">
      FAAB Efficiency
    </MetricHelp>
    <div class="text-xl font-bold mt-2">{faabEfficiency.toFixed(1)}</div>
  </div>
  
  <div class="bg-white p-4 rounded-lg border">
    <MetricHelp metricId="season_consistency_score" position="top">
      Consistency Score
    </MetricHelp>
    <div class="text-xl font-bold mt-2">{consistency.toFixed(3)}</div>
  </div>
</div>
```

## Available Metric IDs

### Manager Performance
- `hall_of_fame_index` - Hall of Fame ranking score
- `career_win_percentage` - Career win percentage
- `faab_efficiency_rating` - FAAB spending efficiency
- `season_consistency_score` - Season-to-season consistency

### Competitiveness
- `competitiveness_index` - Overall league competitiveness
- `win_parity_score` - Win distribution balance
- `point_spread_score` - Scoring distribution tightness

### Head-to-Head Analysis
- `pythagorean_wins` - Expected wins based on scoring
- `luck_factor` - Actual vs expected win difference
- `biggest_win_margin` - Largest victory margin
- `most_lopsided_game` - Biggest point differential

### Trade Analysis
- `production_differential` - Trade value differential
- `trade_winner` - Which team won the trade

### Draft Analysis
- `draft_value_score` - Draft pick value score
- `points_per_week` - Average weekly points

### Player Value
- `points_above_replacement` - Value above replacement level
- `consistency_rating` - Player consistency score

### Team Performance
- `playoff_probability` - Playoff qualification probability
- `power_score` - Team strength rating
- `strength_of_schedule` - Schedule difficulty

### League Analysis
- `waiver_activity_index` - Waiver wire activity level

### Record Book
- Various record metrics for historical achievements

## Styling Guidelines

### Visual Consistency
- Use consistent cursor styles: `cursor-help` for interactive elements
- Add hover effects: `hover:text-blue-600 transition-colors`
- Maintain spacing: tooltips auto-position with proper spacing

### Positioning Tips
- Use `position="bottom"` for header elements
- Use `position="top"` for footer or lower elements  
- Use `position="right"` for left-side elements
- Use `position="left"` for right-side elements

### Responsive Considerations
- Tooltips automatically adjust on mobile devices
- Consider `maxWidth` for narrow containers
- Test tooltip positioning on different screen sizes

## Performance

### Caching
- Metric definitions are automatically cached for 15 minutes
- Cache persists across page navigation
- Common metrics are preloaded for better performance

### Best Practices
- Tooltips load on hover, not on page load
- Related metrics are optional - set `includeRelated={false}` for faster loading
- The system gracefully handles missing or invalid metric IDs

## Accessibility

### Features
- Proper ARIA attributes for screen readers
- Keyboard navigation support (focus/blur events)
- High contrast color schemes
- Semantic markup

### Implementation
- Always include descriptive text in the slot
- Use proper heading structure
- Ensure color isn't the only indicator of meaning

## Troubleshooting

### Common Issues

1. **Tooltip not appearing**
   - Check that `metricId` matches exactly (case-sensitive)
   - Verify the meta-data API is responding
   - Check browser console for errors

2. **Positioning problems**
   - Try different `position` values
   - Adjust `maxWidth` for constrained spaces
   - Ensure parent containers have proper positioning

3. **Performance concerns**
   - Use `includeRelated={false}` for simpler tooltips
   - Consider preloading common metrics
   - Check network tab for excessive API calls

### Debugging
```svelte
<script>
  import { metricDefinitionsStore } from '$lib/stores/metricDefinitions';
  
  // Check cache status
  console.log(metricDefinitionsStore.getCacheStats());
</script>
```

## API Integration

### Adding New Metrics

1. Add metric definition to database:
```sql
INSERT INTO meta_data.metric_definitions (
  metric_id, metric_name, metric_category, short_description, ...
) VALUES (
  'new_metric_id', 'New Metric Name', 'category', 'Description...', ...
);
```

2. Use immediately in components:
```svelte
<MetricHelp metricId="new_metric_id">New Metric</MetricHelp>
```

### API Endpoints
- `GET /api/meta-data?metric_id=X` - Single metric with related metrics
- `GET /api/meta-data?category=X` - All metrics in category
- `GET /api/meta-data?include_categories=true` - All categories
- `GET /api/meta-data` - All metrics grouped by category

## Next Steps

1. **Phase 1** ✅ - Database schema and API
2. **Phase 2** ✅ - Frontend tooltip system  
3. **Phase 3** ⏳ - Admin interface for managing definitions
4. **Phase 4** ⏳ - API documentation integration
5. **Phase 5** ⏳ - Formula display and examples

Start integrating tooltips into existing pages by identifying metrics that would benefit from definitions and adding the appropriate components! 