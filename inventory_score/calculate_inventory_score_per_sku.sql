with historic_data as (

    select distinct
        date,
        parent_id, 
        parent_and_color_manufacturer as parent_and_color, 
        product_id as sku,
        parent_title as title_short,
        size,
        color,
        brand,
        gender,
        stock_total,
        top_category, 
        category_l1_2,
        category_l_2,
        gmc_availability
    from {{ ref('stg_gmc_product_attributes_daily') }}
    where 
        date between {{ get_last_n_days_date_range(180) }} 
        or date = date_sub(current_date(), interval 0 day)

), 

/* Get a shorter time frame to reference against longer term best-case selection */
short_term_data as (

    select *
    from historic_data
    where 
        date between date_sub(current_date(), interval 30 day)
        and date_sub(current_date(), interval 0 day)

),

/* Generate the historic full selection list of SKUs per parent */
historic_max_selection as (

    select distinct
        parent_id, 
        parent_and_color, 
        sku,
        title_short,
        size,
        brand,
        gender
    from historic_data

),

/* Get a full date list to cross join with max selection data */
all_dates as (

    select distinct date
    from historic_data

),

/* Cross join historic max availability with full date range */
historic_max_selection_per_day as (

    select distinct
        all_dates.date,
        historic_max.title_short,
        historic_max.parent_id, 
        historic_max.parent_and_color, 
        historic_max.sku,
        historic_max.size,
        historic_max.brand,
        historic_max.gender,
        count(distinct historic_max.sku) over (partition by historic_max.parent_id) as max_skus_per_parent,
        count(distinct historic_max.sku) over (partition by historic_max.parent_and_color) as max_skus_per_style
    from all_dates
    cross join historic_max_selection as historic_max

),

/* Join historic max availability with recent date range */
skus_with_availability_per_day as (

    select
        historic_max.date, 
        historic_max.parent_id, 
        historic_max.parent_and_color, 
        historic_max.title_short,
        historic_max.sku,
        historic_max.size,
        data.color,
        data.top_category,
        data.category_l1_2,
        data.category_l_2,
        historic_max.max_skus_per_parent,
        historic_max.max_skus_per_style,
        historic_max.gender,
        data.gmc_availability
    from historic_max_selection_per_day as historic_max
    left join short_term_data as data using (date, sku)
    where 
        date between date_sub(current_date(), interval 30 day)
        and date_sub(current_date(), interval 0 day)

),

/* Add row number for parent ids and color styles */
skus_with_rankings as (

    select 
        *,
        row_number() over (partition by date, parent_id order by sku) as rank_per_parent,
        row_number() over (partition by date, parent_and_color order by sku) as rank_per_style,
        count(gmc_availability) over (partition by date, parent_id) as max_skus_per_day_and_parent,
        count(gmc_availability) over (partition by date, parent_and_color) as max_skus_per_day_and_style
    from skus_with_availability_per_day
    order by date, parent_and_color, sku

),

/* Apply the 1-2-2-1 weights per sku by calculating distance for the center */
skus_with_distance_values as (

    select 
        *,
        round(safe_divide(max_skus_per_day_and_parent, max_skus_per_parent), 2) as skus_in_stock_to_parent_all,
        round(safe_divide(max_skus_per_day_and_style, max_skus_per_style), 2) as skus_in_stock_to_style_all,
        max(rank_per_style) over (partition by date, parent_and_color) as max_value_per_style,
        /* Multiplying by ten to receive integer values, adding to divide the odd or even number sequence */
        max_skus_per_style/2*10+5 as middle_value_times_10_style,
    from skus_with_rankings

), 

/* Calculate the absolute distance from the middle value to the periphery, while avoiding decimal points */
skus_with_weights as (

    select 
        *, 
        abs(rank_per_style*10 - middle_value_times_10_style) as diff_to_middle_value_times_10_style,
        case 
            when abs(rank_per_style*10 - middle_value_times_10_style) <= middle_value_times_10_style / 2 then 2
            else 1
        end as sku_weight_style
    from skus_with_distance_values

),

/* Calculuating the sum of weights for available skus and the total sum of weights */
skus_with_weighted_sum as (

    select 
        *,
        /* Only calculate a score for items in stock */ 
        case 
            when gmc_availability = 'in stock'
            then sum(sku_weight_style) over (partition by date, parent_and_color, gmc_availability) 
            else null
        end as available_sku_weight_sum_style,
        sum(sku_weight_style) over (partition by date, parent_and_color) as total_sku_weight_sum_style
    from skus_with_weights

),

/* Dividing the sum of weights of available skus by the total sum of weights */
skus_with_weighted_inventory_score as (

    select
        *, 
        case
            /* For kids sizes, all sizes are treated equal, as kids grow through all sizes */
            when gender = 'Kinder' then skus_in_stock_to_style_all
            /* For adult sizes, the 1-2-2-1 weighted sum calculation is applied */
            else round(safe_divide(available_sku_weight_sum_style, total_sku_weight_sum_style),2) 
        end as weighted_inventory_score
    from skus_with_weighted_sum

)

select 
    *,
    case
        when weighted_inventory_score is null then null
        when weighted_inventory_score < 0.1 then "0-9 %"
        when weighted_inventory_score >= 0.1 and weighted_inventory_score < 0.2 then "10-19 %"
        when weighted_inventory_score >= 0.2 and weighted_inventory_score < 0.3 then "20-29 %"
        when weighted_inventory_score >= 0.3 and weighted_inventory_score < 0.4 then "30-39 %"
        when weighted_inventory_score >= 0.4 and weighted_inventory_score < 0.5 then "40-49 %"
        when weighted_inventory_score >= 0.5 and weighted_inventory_score < 0.6 then "50-59 %"
        when weighted_inventory_score >= 0.6 and weighted_inventory_score < 0.7 then "60-69 %"
        when weighted_inventory_score >= 0.7 and weighted_inventory_score < 0.8 then "70-79 %"
        when weighted_inventory_score >= 0.8 and weighted_inventory_score < 0.9 then "80-89 %"
        else "90- 100%"
    end as weighted_inv_score_grouped 
from skus_with_weighted_inventory_score