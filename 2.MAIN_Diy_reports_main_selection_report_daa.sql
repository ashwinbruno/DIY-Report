## DIY Reports -- 

--> SP -- sp_od_qt_select_report_data

DROP PROCEDURE IF EXISTS sp_od_qt_select_report_data;
CREATE PROCEDURE sp_od_qt_select_report_data
(
    arg_user_id INT(11) UNSIGNED, 
    arg_product_ids VARCHAR(255),
    arg_channel_ids VARCHAR(255),
    arg_tab_ids VARCHAR(255),
    arg_start_day_id DATE, 
    arg_end_day_id DATE, 
    arg_interval_type_id INT(11) UNSIGNED, 
    arg_filter_param MEDIUMTEXT, /*dimension_id:include_flag:value&@&dimension_id:include_flag:value*/

    arg_dimension_ids VARCHAR(255), 
    arg_metric_ids MEDIUMTEXT,
    arg_currency_id INT(11) UNSIGNED,
    arg_sort_param VARCHAR(255), 
    arg_start_limit INT(11) UNSIGNED, 
    arg_end_limit INT(11) UNSIGNED, 
    arg_limit_flag TINYINT(1) UNSIGNED 
)
BEGIN
    
    DECLARE cur_product_code,cur_product_id,cur_channel,cur_channel_id,cur_where_main,cur_where_condition,cur_account_ids_list,cur_partion_name_list,cur_join_cond,cur_select_interval_field,cur_select_entity_part,cur_select_cond,cur_group_by_interval_field,cur_group_by_cond,cur_remaining_part MEDIUMTEXT;
    
    DECLARE cur_generate_inside_tables CURSOR FOR 
    SELECT product_code,product_id,channel,channel_id,where_main,where_condition,account_ids_list,partion_name_list,join_cond,select_interval_field,select_entity_part,select_cond,group_by_interval_field,group_by_cond,remaining_part
    FROM od_qt_prepare_report_metadata;
    
    DROP TEMPORARY TABLE IF EXISTS od_qt_final_data_temp;
    
    DROP TEMPORARY TABLE IF EXISTS od_tmp_daywise_rev;
    CREATE TEMPORARY TABLE od_tmp_daywise_rev
    (
        day_id DATE,
        site_id INT(11) UNSIGNED,
        product_id INT(11) UNSIGNED,
        channel_id INT(11) UNSIGNED,
        revenue DECIMAL(58,10)
    );
          
    SET @var_no_of_month_interval = 3;
    SET @var_diff_in_months = (TIMESTAMPDIFF(MONTH,arg_start_day_id,arg_end_day_id)+1);
    SET @var_check_huge_data_flag = IF( @var_diff_in_months > @var_no_of_month_interval,1,0);
    
    SET @var_max_loop = CEIL(@var_diff_in_months/@var_no_of_month_interval); 
    SET @var_start_loop = 1;
    
    SET @var_start_day_id_loop = arg_start_day_id; 
    
    SET @var_end_day_id_loop   = IF(arg_end_day_id < LAST_DAY(DATE_SUB(DATE_ADD(@var_start_day_id_loop,INTERVAL @var_no_of_month_interval MONTH),INTERVAL 1 MONTH)),arg_end_day_id,LAST_DAY(DATE_SUB(DATE_ADD(@var_start_day_id_loop,INTERVAL @var_no_of_month_interval MONTH),INTERVAL 1 MONTH))) ;/*2020-12-30*/


    WHILE (@var_end_day_id_loop < arg_end_day_id OR @var_start_loop = 1) DO
      
          SET @var_start_day_id_loop = IF(@var_start_loop = 1,@var_start_day_id_loop,  DATE_ADD(LAST_DAY(DATE_SUB(DATE_ADD(@var_start_day_id_loop,INTERVAL @var_no_of_month_interval MONTH),INTERVAL 1 MONTH)),INTERVAL 1 DAY) );/*2020-01-15*/
          
          SET @var_end_day_id_loop   = IF(arg_end_day_id < LAST_DAY(DATE_SUB(DATE_ADD(@var_start_day_id_loop,INTERVAL @var_no_of_month_interval MONTH),INTERVAL 1 MONTH)),arg_end_day_id,LAST_DAY(DATE_SUB(DATE_ADD(@var_start_day_id_loop,INTERVAL @var_no_of_month_interval MONTH),INTERVAL 1 MONTH))) ;/*2020-12-30*/
    
          SET @var_geo_device_flag = "";
          SET @var_partner_flag = "";
          SET @where_main = "";
          SET @where_cond = "";
          SET @var_sort_param = "";
          SET @var_limit_part = "";
          SET @var_select_day_part = "";
          SET @var_group_by_day_part = "";
          SET @var_current_row = 1;
          SET @var_total_rows = "";
          SET @var_select_day_overall = "";
          SET @var_dimension_select_main_overall = "";
          SET @var_dimension_select_formula_overall = "";
          SET @var_dimension_select_total_sum_formula = "";
          SET @var_group_by_day_overall = "";
          SET @var_od_column_flag = "";



          SET @var_select_columns = "";
          SET @var_select_day_mid_part = "";
          SET @var_group_by_day_mid_part = "";
          SET @var_dimension_select_formula_final = "";
          SET @var_dimension_select_total_sum_formula_2 = "";
          SET @var_dimension_select_main_final = "";
          SET @var_select_day_final = "";
          SET @var_ga_column_flag = "";
          SET @where_ga_cond = "";
          SET @var_join_ga = "";
          SET @var_ga_join_on = "";
--           SET @var_ga_count = 0;
          SET @var_display_overview_network_ecpm = 0;

          
          SET @var_geo_device_flag = (SELECT IF(COUNT(1) > 0,1,0) FROM od_qt_dimension_master WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code IN ('geo','device_type') AND active_status = 1);
          
          SET @var_partner_flag = (SELECT IF(COUNT(1) > 0,1,0) FROM od_qt_dimension_master WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND partner_flag = 1 AND active_status = 1);
          
          SET @var_od_column_flag = (SELECT IF(COUNT(1) > 0,1,0) FROM od_qt_metric_master WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND od_column_flag = 1 AND active_status = 1);
          
          
          SET @var_ga_column_flag = (SELECT IF(COUNT(1) > 0,1,0) FROM od_qt_metric_master WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND od_column_flag IN (2,3) AND active_status = 1);

          SET @var_benchmark_comp_flag = (SELECT IF(COUNT(1) > 0,1,0) FROM od_qt_metric_master WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND metric_code = "benchmark_comparison_per_display" AND active_status = 1);

          
          SET @where_main = CONCAT(" WHERE header.day_id BETWEEN '",@var_start_day_id_loop,"' AND '",@var_end_day_id_loop,"' AND account.account_id NOT IN (31,62) ");
          
          SET @var_sort_param = IF(arg_sort_param = "","",CONCAT(" ORDER BY ",arg_sort_param));
          
          SET @var_limit_part = IF(arg_limit_flag = 1,CONCAT(" LIMIT ",arg_start_limit,",",arg_end_limit),"");
          
          
--           SET @var_dfp_view_flag = "";
          SET @var_dfp_view_flag_overall_network = "";
          SET @var_dfp_view_flag_overall_non_gam = "";
          
          SET @var_dfp_view_flag_display_network = "";
          SET @var_dfp_view_flag_display_non_gam = "";
          
          SET @var_dfp_view_flag_native_network = "";
          SET @var_dfp_view_flag_native_non_gam = "";
                    
          SET @var_dfp_view_flag_video_network = "";
          SET @var_dfp_view_flag_video_non_gam = "";

          SET @var_dfp_view_flag_app_network = "";
          SET @var_dfp_view_flag_app_non_gam = "";
          
          -- ## Tab Filter
          SET @var_tab_code = "";
--           SET @var_where = "";
          SET @var_where_display = "";
          SET @var_where_native = "";
          SET @var_where_video = "";
          SET @var_where_youtube = "";
          SET @var_where_app = "";
          
          SET @var_neglect_unfilled_numbers = 0;
          SET @var_viewability_where = "";

          SET @var_where_display_app_overall = "";
          SET @var_where_native_overall = "";
          SET @var_where_video_overall = "";
          SET @var_where_youtube_overall = "";          
          
          SELECT GROUP_CONCAT(tab_code) INTO @var_tab_code 
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND active_status = 1;          

          SELECT dfp_view_flag INTO @var_dfp_view_flag_overall_network 
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND tab_code = "overall_network" AND active_status = 1;          
          
          SELECT dfp_view_flag INTO @var_dfp_view_flag_overall_non_gam
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND tab_code = "overall_nongam" AND active_status = 1;          
            
            
          SELECT dfp_view_flag INTO @var_dfp_view_flag_display_network 
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND tab_code = "display_network" AND active_status = 1;          
          
          SELECT dfp_view_flag INTO @var_dfp_view_flag_display_non_gam
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND tab_code = "display_nongam" AND active_status = 1;          
                           

          SELECT dfp_view_flag INTO @var_dfp_view_flag_native_network 
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND tab_code = "native_network" AND active_status = 1;          
          
          SELECT dfp_view_flag INTO @var_dfp_view_flag_native_non_gam
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND tab_code = "native_nongam" AND active_status = 1;          
                                               
                    
          SELECT dfp_view_flag INTO @var_dfp_view_flag_video_network 
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND tab_code = "video_network" AND active_status = 1;          
          
          SELECT dfp_view_flag INTO @var_dfp_view_flag_video_non_gam
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND tab_code = "video_nongam" AND active_status = 1;          
                         
                         
          SELECT dfp_view_flag INTO @var_dfp_view_flag_app_network 
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND tab_code = "app_network" AND active_status = 1;          
          
          SELECT dfp_view_flag INTO @var_dfp_view_flag_app_non_gam
          FROM od_dash_tab_master 
          WHERE FIND_IN_SET(tab_id,arg_tab_ids) > 0 AND tab_code = "app_nongam" AND active_status = 1;          
                                                           
          
                    -- ## Tab Filter
          -- ## Overall 
          IF( FIND_IN_SET("overall_overview",@var_tab_code) > 0) THEN 
              SET @var_where_display_app_overall = CONCAT(@var_where_display_app_overall,"");
            
              SET @var_where_native_overall  = CONCAT(@var_where_native_overall,"");
              SET @var_where_video_overall   = CONCAT(@var_where_video_overall,"");
              SET @var_where_youtube_overall = CONCAT(@var_where_youtube_overall,"");
                         
          ELSEIF (FIND_IN_SET("overall_network",@var_tab_code) > 0 ) THEN
          
              SET @var_where_display_app_overall = CONCAT(@var_where_display_app_overall," AND (( ",(CASE @var_dfp_view_flag_overall_network 
                                                                WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END),"
              
                                                        AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'
                                                        AND IFNULL(adunit.sub_channel_id,0) != 5 /*Exclude RichMedia*/ )
                                                        OR IFNULL(provider.provider_code,'') = 'provider_affinity_hvr'
                                                      ) ");
                
              SET @var_where_native_overall = CONCAT(@var_where_native_overall," AND (( ",(CASE @var_dfp_view_flag_overall_network 
                                                                WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END),"
              
                                                        AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'
                                                                                       )
                                                        OR IFNULL(provider.provider_code,'') = 'provider_affinity_hvr'
                                                      ) ");                
                                                      
              SET @var_where_video_overall = CONCAT(@var_where_video_overall," AND (( ",(CASE @var_dfp_view_flag_overall_network 
                                                                WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END),"
              
                                                        AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'
                                                                                       )
                                                        OR IFNULL(provider.provider_code,'') = 'provider_affinity_hvr'
                                                      ) ");

              SET @var_where_youtube_overall = CONCAT(@var_where_youtube_overall," AND (( ",(CASE @var_dfp_view_flag_overall_network 
                                                                WHEN 1 THEN " IFNULL(adunit.video_id,0) > 0 " 
                                                                WHEN 2 THEN " IFNULL(adunit.video_id,0) > 0 " 
                                                                WHEN 3 THEN " IFNULL(adunit.video_id,0) < 0 " ELSE " " END),"
              
                                                        AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'
                                                                                       )
                                                        OR IFNULL(provider.provider_code,'') = 'provider_affinity_hvr'
                                                      ) ");
                                                      
              SET @var_viewability_where = CONCAT("AND IFNULL(adunit.sub_channel_id,0) != 5");     
          
          
          ELSEIF (FIND_IN_SET("overall_direct",@var_tab_code) > 0 ) THEN 
              SET @var_where_display_app_overall = CONCAT(@var_where_display_app_overall," AND IFNULL(prov_category.provider_category_type_name,'') = 'Direct' /*AND adunit.sub_channel_id != 5*/");
              
              SET @var_where_native_overall = CONCAT(@var_where_native_overall," AND IFNULL(prov_category.provider_category_type_name,'') = 'Direct' /*AND adunit.sub_channel_id != 5*/");
              SET @var_where_video_overall = CONCAT(@var_where_video_overall," AND IFNULL(prov_category.provider_category_type_name,'') = 'Direct' /*AND adunit.sub_channel_id != 5*/");
              SET @var_where_youtube_overall = CONCAT(@var_where_youtube_overall," AND IFNULL(prov_category.provider_category_type_name,'') = 'Direct' /*AND adunit.sub_channel_id != 5*/");
              
              
              SET @var_neglect_unfilled_numbers = 1;
           
           
              
          ELSEIF (FIND_IN_SET("overall_richmedia",@var_tab_code) > 0 ) THEN
              SET @var_where_display_app_overall = CONCAT(@var_where_display_app_overall," AND IFNULL(adunit.sub_channel_id,0) = 5 AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'");

--               SET @var_where_native_overall = CONCAT(@var_where_native_overall," AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'");
--               SET @var_where_video_overall = CONCAT(@var_where_video_overall," AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'");
--               SET @var_where_youtube_overall = CONCAT(@var_where_youtube_overall," AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'");              
--               
              
              SET @var_where_native_overall = CONCAT(@var_where_native_overall," AND 1<>1");
              SET @var_where_video_overall = CONCAT(@var_where_video_overall," AND 1<>1");
              SET @var_where_youtube_overall = CONCAT(@var_where_youtube_overall," AND 1<>1");              
              
              
              SET @var_viewability_where = CONCAT("AND IFNULL(adunit.sub_channel_id,0) = 5");
          

          ELSEIF (FIND_IN_SET("overall_nongam",@var_tab_code) > 0 ) THEN
              SET @var_where_display_app_overall = CONCAT(@var_where_display_app_overall," AND (",(CASE @var_dfp_view_flag_overall_non_gam 
                                                          WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0  " 
                                                          WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0  " 
                                                          WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0  " ELSE " " END)," 
                                                      
                                                      AND IFNULL(provider.provider_code,'') != 'provider_affinity_hvr') ");

              SET @var_where_native_overall = CONCAT(@var_where_native_overall," AND (",(CASE @var_dfp_view_flag_overall_non_gam 
                                                                    WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0  " 
                                                                    WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0  " 
                                                                    WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0  " ELSE " " END)," 
                                                                
                                                                AND IFNULL(provider.provider_code,'') != 'provider_affinity_hvr') ");
              SET @var_where_video_overall = CONCAT(@var_where_video_overall," AND (",(CASE @var_dfp_view_flag_overall_non_gam 
                                                          WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0  " 
                                                          WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0  " 
                                                          WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0  " ELSE " " END)," 
                                                      
                                                      AND IFNULL(provider.provider_code,'') != 'provider_affinity_hvr') ");
              SET @var_where_youtube_overall = CONCAT(@var_where_youtube_overall," AND (",(CASE @var_dfp_view_flag_overall_non_gam 
                                                          WHEN 1 THEN " IFNULL(adunit.video_id,0) > 0  " 
                                                          WHEN 2 THEN " IFNULL(adunit.video_id,0) > 0  " 
                                                          WHEN 3 THEN " IFNULL(adunit.video_id,0) < 0  " ELSE " " END)," 
                                                      
                                                      AND IFNULL(provider.provider_code,'') != 'provider_affinity_hvr') ");
                                                                                          
          END IF;
          
          -- ## Display 
          
          IF (FIND_IN_SET("display_overview",@var_tab_code) > 0 ) THEN
              SET @var_where_display = CONCAT(@var_where_display,"");
          
          
          
          ELSEIF (FIND_IN_SET("display_network",@var_tab_code) > 0 ) THEN
              SET @var_where_display = CONCAT(@var_where_display,"  (( ",(CASE @var_dfp_view_flag_display_network 
                                                                WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END),"
              
                                                        AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'
                                                        AND IFNULL(adunit.sub_channel_id,0) != 5 /*Exclude RichMedia*/ )
                                                        OR IFNULL(provider.provider_code,'') = 'provider_affinity_hvr'
                                                      ) ");
                                                      
              SET @var_viewability_where = CONCAT("AND IFNULL(adunit.sub_channel_id,0) != 5");     
              
              

          ELSEIF (FIND_IN_SET("display_direct",@var_tab_code) > 0 ) THEN 
              SET @var_where_display = CONCAT(@var_where_display,"  IFNULL(prov_category.provider_category_type_name,'') = 'Direct' /*AND adunit.sub_channel_id != 5*/");
                        
              SET @var_neglect_unfilled_numbers = 1;
              
          
          
          ELSEIF (FIND_IN_SET("display_richmedia",@var_tab_code) > 0 ) THEN
              SET @var_where_display = CONCAT(@var_where_display," IFNULL(adunit.sub_channel_id,0) = 5 AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'");
              
              SET @var_viewability_where = CONCAT("AND IFNULL(adunit.sub_channel_id,0) = 5");
                                                      
          ELSEIF (FIND_IN_SET("display_nongam",@var_tab_code) > 0 ) THEN
              SET @var_where_display = CONCAT(@var_where_display," (",(CASE @var_dfp_view_flag_display_non_gam 
                                                          WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                          WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                          WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END)," 
                                                      
                                                      AND IFNULL(provider.provider_code,'') != 'provider_affinity_hvr') ");
                      
          END IF;
          
          
          -- ## Native
          IF (FIND_IN_SET("native_overview",@var_tab_code) > 0 ) THEN
              SET @var_where_native = CONCAT(@var_where_native,"");    
          
          
                        
          ELSEIF (FIND_IN_SET("native_network",@var_tab_code) > 0 ) THEN
              SET @var_where_native = CONCAT(@var_where_native," AND ( ",(CASE @var_dfp_view_flag_native_network 
                                                              WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                              WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                              WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END),"
                                                    ) ");
                                                    
          
          
          ELSEIF (FIND_IN_SET("native_nongam",@var_tab_code) > 0 ) THEN
              SET @var_where_native = CONCAT(@var_where_native," AND (",(CASE @var_dfp_view_flag_native_non_gam
                                                        WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                        WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                        WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END)," 
                                                    ) ");
          
          END IF;
          
          
          
          IF (FIND_IN_SET("video_overview",@var_tab_code) > 0 ) THEN
              SET @var_where_video = CONCAT(@var_where_video,"");              

          ELSEIF (FIND_IN_SET("video_network",@var_tab_code) > 0 ) THEN
                SET @var_where_video = CONCAT(@var_where_video," AND ( ",(CASE @var_dfp_view_flag_video_network 
                                                              WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                              WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                              WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END),"
                                                    ) ");
                                                    

                                                                                          
          ELSEIF (FIND_IN_SET("video_nongam",@var_tab_code) > 0 ) THEN
                SET @var_where_video = CONCAT(@var_where_video," AND (",(CASE @var_dfp_view_flag_video_non_gam 
                                                        WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                        WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                        WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END)," 
                                                    
                                                    ) ");
                                                    
          END IF;
          
          IF (FIND_IN_SET("youtube_overview",@var_tab_code) > 0 ) THEN
              SET @var_where_youtube = CONCAT(@var_where_youtube,"");
          END IF;
          
          
          
          IF (FIND_IN_SET("app_overview",@var_tab_code) > 0 ) THEN
              SET @var_where_app = CONCAT(@var_where_app,"");
          
          
          ELSEIF (FIND_IN_SET("app_network",@var_tab_code) > 0 ) THEN
          
              SET @var_where_app = CONCAT(@var_where_app," AND (( ",(CASE @var_dfp_view_flag_app_network 
                                                                WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END),"
              
                                                        AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'
                                                        AND IFNULL(adunit.sub_channel_id,0) != 5 /*Exclude RichMedia*/ )
                                                        OR IFNULL(provider.provider_code,'') = 'provider_affinity_hvr'
                                                      ) ");
                                                      
              SET @var_viewability_where = CONCAT("AND IFNULL(adunit.sub_channel_id,0) != 5");     
                  

          ELSEIF (FIND_IN_SET("app_nongam",@var_tab_code) > 0 ) THEN 
              SET @var_where_app = CONCAT(@var_where_app," AND (",(CASE @var_dfp_view_flag_app_non_gam
                                                          WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                          WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                          WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END)," 
                                                      
                                                      AND IFNULL(provider.provider_code,'') != 'provider_affinity_hvr') ");
          END IF;
          
          SELECT interval_type_code INTO @var_interval_type_code
          FROM od_qt_interval_type_master
          WHERE interval_type_id = arg_interval_type_id;
          
          SET @var_daily_interval_type_flag = IF(@var_interval_type_code = 'day',1,0);
          
          SET @var_select_day_part = CONCAT(
                                        (CASE @var_interval_type_code 
                                            WHEN "day" THEN "header.day_id AS day_id," 
                                            WHEN "month" THEN "YEAR(header.day_id) AS year_id,MONTH(header.day_id) AS month_id,MONTHNAME(header.day_id) AS month_name,DATE_FORMAT(CONCAT(YEAR(header.day_id),'-',MONTH(header.day_id),'-01'),'%Y, %M') AS formated_year_month," 
                                            WHEN "cumulative" THEN " " 
                                        END));
                                        
          SET @var_group_by_day_part = CONCAT("GROUP BY ",
                                              (CASE @var_interval_type_code 
                                                  WHEN "day" THEN "header.day_id," 
                                                  WHEN "month" THEN "YEAR(header.day_id),MONTH(header.day_id)," 
                                                  WHEN "cumulative" THEN " " 
                                              END));
          
          SET @var_select_day_overall = CONCAT(
                                        (CASE @var_interval_type_code 
                                            WHEN "day" THEN "overall_data.day_id AS dayid," 
                                            WHEN "month" THEN "overall_data.year_id,overall_data.month_id,overall_data.month_name,overall_data.formated_year_month," 
                                            WHEN "cumulative" THEN CONCAT("'",@var_start_day_id_loop," - ",@var_end_day_id_loop,"' AS date_range,")
                                        END));
                                        
          SET @var_select_day_final = CONCAT(
                                        (CASE @var_interval_type_code 
                                            WHEN "day" THEN "overall_data.dayid AS dayid," 
                                            WHEN "month" THEN "overall_data.year_id,overall_data.month_id,overall_data.month_name,overall_data.formated_year_month," 
                                            WHEN "cumulative" THEN CONCAT("'",@var_start_day_id_loop," - ",@var_end_day_id_loop,"' AS date_range,")
                                        END));
                                        
          SET @var_group_by_day_overall = CONCAT("GROUP BY ",
                                              (CASE @var_interval_type_code 
                                                  WHEN "day" THEN "overall_data.day_id," 
                                                  WHEN "month" THEN "overall_data.year_id,overall_data.month_id," 
                                                  WHEN "cumulative" THEN " " 
                                              END));
                                              
          SET @var_select_day_mid_part = CONCAT(
                                        (CASE @var_interval_type_code 
                                            WHEN "day" THEN "header.day_id AS day_id," 
                                            WHEN "month" THEN " YEAR(header.day_id) AS year_id,MONTH(header.day_id) AS month_id,MONTHNAME(header.day_id) AS month_name,DATE_FORMAT(CONCAT(YEAR(header.day_id),'-',MONTH(header.day_id),'-01'),'%Y, %M') AS formated_year_month," 
                                            WHEN "cumulative" THEN " " 
                                        END));
                                        
          SET @var_group_by_day_mid_part = CONCAT("GROUP BY ",
                                              (CASE @var_interval_type_code 
                                                  WHEN "day" THEN "header.day_id," 
                                                  WHEN "month" THEN " YEAR(header.day_id),MONTH(header.day_id)," 
                                                  WHEN "cumulative" THEN " " 
                                              END));

          
          
            
      --     ############ Used For Unfilled ##############
      --     
      --     SET @var_interval_type = @var_interval_type_code;
      --     
      --     IF (@var_interval_type_code IN ("month","cumulative")) THEN
      --     
      --           SET @var_start_date = @var_start_day_id_loop;
      --           SET @var_end_date = @var_end_day_id_loop;
      --     END IF;
      --     
      --     SET @var_dimension_list = (SELECT GROUP_CONCAT(dimension_code) FROM od_qt_dimension_master WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code IN ("publisher","site","adunit") AND active_status = 1);
      --     
      --     #############################################
      
              
          DROP TEMPORARY TABLE IF EXISTS od_qt_prepare_report_all_combination;
          CREATE TEMPORARY TABLE od_qt_prepare_report_all_combination(
          master_id INT(11) UNSIGNED PRIMARY KEY AUTO_INCREMENT,
          product_code VARCHAR(30),
          product_id INT(11) UNSIGNED,
          channel VARCHAR(30),
          channel_id INT(11) UNSIGNED,
          table_name VARCHAR(255)
          );
          
          
          

          SELECT dfp_view_flag INTO @var_dfp_view_flag_display_network_benchmark_comp 
          FROM od_dash_tab_master 
          WHERE tab_code = "display_network" AND active_status = 1;          
                    
          SET @var_where_display_network_benchmark_comp = "";
          IF(@var_benchmark_comp_flag = 1) THEN
              SET @var_where_display_network_benchmark_comp = CONCAT(@var_where_display_network_benchmark_comp," AND (( ",(CASE @var_dfp_view_flag_display_network_benchmark_comp 
                                                                            WHEN 1 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                            WHEN 2 THEN " IFNULL(adunit.ad_unit_id,0) > 0 " 
                                                                            WHEN 3 THEN " IFNULL(adunit.ad_unit_id,0) < 0 " ELSE " " END),"
                        
                                                                    AND IFNULL(prov_category.provider_category_type_name,'') != 'Direct'
                                                                    AND IFNULL(adunit.sub_channel_id,0) != 5 /*Exclude RichMedia*/ )
                                                                    OR IFNULL(provider.provider_code,'') = 'provider_affinity_hvr'
                                                                ) ");          
          END IF;
          
 -- ------------------------- HB WHERE ----------------------------
          SET @var_dimension_check_HB_Region = 0;
          
          SET @var_filter_param_hb = CONCAT(arg_filter_param,"&@&");
          
          WHILE (@var_filter_param_hb != "") DO
          
              SET @var_current_filter_param_hb = SUBSTRING_INDEX(@var_filter_param_hb,"&@&",1);
              SET @var_dimension_id = SUBSTRING_INDEX(@var_current_filter_param_hb,":",1);
              
              SELECT COUNT(1) INTO @var_filter_check_HB_Region
              FROM od_qt_dimension_master 
              WHERE dimension_id = @var_dimension_id 
              AND active_status = 1
              AND dimension_where_part = "region.region_id";
              
              SELECT COUNT(1) INTO @var_dimension_check_HB_Region
              FROM od_qt_dimension_master
              WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND active_status = 1 AND dimension_code LIKE '%hb_region%';
              
              IF (@var_filter_check_HB_Region > 0 OR @var_dimension_check_HB_Region > 0 ) THEN
           
                  SET @var_dimension_check_HB_Region = 1;
          
              END IF;
              
              SET @var_filter_param_hb = REPLACE(@var_filter_param_hb,CONCAT(@var_current_filter_param_hb,"&@&"),"");
          END WHILE;

         
          
          SET @var_table_name_analytics_header = "";
          SET @var_table_name_analytics_header  = IF(@var_dimension_check_HB_Region = 0, "od_account_hb_intermediate_data_report_header","od_account_hb_data_report_header");

          SET @var_table_name_analytics_details = "";
          SET @var_table_name_analytics_details = IF(@var_dimension_check_HB_Region = 0, "od_account_hb_intermediate_data_report_details","od_account_hb_data_report_details");
        
--           SELECT @var_dimension_check_HB_Region,@var_table_name_analytics_header;
          
          INSERT INTO od_qt_prepare_report_all_combination VALUES 
          (NULL,"analytics"     ,1,"Display" ,1  ,"od_account_payment_intermediate_data_report_header"),
          (NULL,"analytics"     ,1,"Native"  ,2  ,"od_account_native_data_report_header"),
          (NULL,"analytics"     ,1,"Video"   ,3  ,"od_account_video_data_report_header"),
          (NULL,"analytics"     ,1,"Youtube" ,5  ,"od_account_youtube_data_report_header"),
          (NULL,"analytics"     ,1,"App"     ,6  ,"od_account_payment_intermediate_data_report_header"),
          (NULL,"headerbidding" ,2,""        ,0  , @var_table_name_analytics_header)/*,
          (NULL,"analytics"     ,1,""        ,0  ,"od_ga_account_site_viewid_report_data"),
          
          (NULL,"analytics"     ,1,""        ,0  ,"od_mi_daily_account_detailed_view_data_wo_direct"),
          (NULL,"analytics"     ,1,""        ,0  ,"od_mi_daily_account_detailed_view_data_with_direct")*/;
          
--           SELECT * FROM od_qt_prepare_report_all_combination;
          
          DROP TEMPORARY TABLE IF EXISTS od_qt_prepare_report_input_combination;
          CREATE TEMPORARY TABLE od_qt_prepare_report_input_combination LIKE od_qt_prepare_report_all_combination;
          
          SET @var_product_ids = CONCAT(arg_product_ids,",");
          SET @var_channel_ids = arg_channel_ids;
          SET @all_comb = "";
          WHILE (@var_product_ids != "") DO
              SET @var_current_product_id = SUBSTRING_INDEX(@var_product_ids,",",1);
              
              IF (@var_current_product_id = 1) THEN 
                  SET @all_comb = CONCAT(@all_comb,",",@var_current_product_id,"-",REPLACE(@var_channel_ids,",",CONCAT(",",@var_current_product_id,"-")));
              ELSE
                  SET @all_comb = CONCAT(@all_comb,",",@var_current_product_id,"-","0");
              END IF;
      --         SELECT @all_comb;
              SET @var_product_ids = REPLACE(@var_product_ids,CONCAT(@var_current_product_id,","),"");
          END WHILE;
          
          SET @all_comb = TRIM(LEADING "," FROM @all_comb);
      --     SELECT @all_comb;

          SET @sql_get_combinations = CONCAT("INSERT INTO od_qt_prepare_report_input_combination
                                              SELECT NULL,product_code,product_id,channel,channel_id,table_name
                                              FROM od_qt_prepare_report_all_combination
                                              WHERE FIND_IN_SET(CONCAT(product_id,'-',channel_id),'",@all_comb,"') > 0 ");
                                              
          PREPARE STMT FROM @sql_get_combinations;
          EXECUTE STMT;
          DEALLOCATE PREPARE STMT;
          
          
          DROP TEMPORARY TABLE IF EXISTS od_qt_prepare_report_metadata;
          CREATE TEMPORARY TABLE od_qt_prepare_report_metadata(
          master_id INT(11) UNSIGNED PRIMARY KEY AUTO_INCREMENT,
          product_code VARCHAR(30),
          product_id TINYINT(3) UNSIGNED,
          channel VARCHAR(30),
          channel_id TINYINT(3) UNSIGNED,
          where_main MEDIUMTEXT,
          where_condition MEDIUMTEXT,
          account_ids_list MEDIUMTEXT,
          partion_name_list MEDIUMTEXT,
          join_cond MEDIUMTEXT,
          select_interval_field MEDIUMTEXT,
          select_entity_part MEDIUMTEXT,
          select_cond MEDIUMTEXT,
          group_by_interval_field MEDIUMTEXT,
          group_by_cond MEDIUMTEXT,
          remaining_part MEDIUMTEXT
          );
          
      --     OPEN cur_select_comb;
      --     SELECT FOUND_ROWS() INTO @var_total_rows;
          SELECT COUNT(1) INTO @var_total_rows FROM od_qt_prepare_report_input_combination;
          
          WHILE (@var_current_row <= @var_total_rows) DO

              #################################################

              SET @var_product_id = "";
              SET @var_channel_id = "";
              SET @var_count = "";
              SET @var_accountids_list_final = "";
              SET @var_current_filter_param = "";
              SET @var_dimension_id = "";
              SET @var_include_flag = "";
              SET @var_filter_value = "";
              SET @var_dimension_where_name = "";
              SET @var_account_ids_list_include = "";
              SET @var_account_ids_list_exclude = "";
              SET @var_partion_name_list_main = "";
              SET @var_partion_name_list_adunit = "";
              SET @var_partion_name_list_crt = "";
              SET @var_partion_name_list_add = "";
              
              SET @var_partion_name_list_native_add = "";
              SET @var_partion_name_list_video_add = "";
              
              SET @var_extra_dimension_join = "";
              SET @var_join = "";
              SET @var_dimension_select_main_part = "";
              SET @var_select_formula_part = "";
              SET @sql_get_account_ids = "";
              SET @var_select_extra_columns = "";
              SET @var_select_payment_part = "";
              SET @var_join_cond_payment_part = "";
              SET @var_od_join = "";
              SET @var_unfilled_join_on = "";
              SET @var_view_per_join_on = "";
              SET @unfilled_part = "";
              SET @viewable_part = "";
              SET @var_select_columns_unfilled = "";
              SET @var_remaining_part = "";
              SET @var_select_columns_inner = "";
              SET @var_dimension_select_main_mid_part = "";
              SET @var_extra_dimension_mid_group_by = "";
              SET @var_select_mid_columns = "";
              SET @var_select_columns_viewability = "";
              SET @var_select_overall_columns_viewability = "";
              SET @remaining_viewability_part = "";
              SET @where_modifed = "";
              SET @where_source_cond = "";
              
                
              
              SELECT product_code,product_id,channel,channel_id,table_name
              INTO @var_product_code,@var_product_id,@var_channel,@var_channel_id,@var_table_name
              FROM od_qt_prepare_report_input_combination
              WHERE master_id = @var_current_row;
              
              SET @where_cond = IF(@var_channel_id IN (1,2,3,6),CONCAT(" AND (site_chn_map.channel_id = ",@var_channel_id,")"),"");
--               SET @where_source_cond = (CASE @var_dfp_view_flag WHEN '2' AND @var_tab_code NOT LIKE '%network%' THEN ' AND adunit.ad_unit_id > 0 ' WHEN '3' THEN ' AND adunit.ad_unit_id < 0 ' ELSE ' ' END);
              -- ------------------------- WHERE ----------------------------
              
              SET @var_filter_param = CONCAT(arg_filter_param,"&@&");
              
      --         SELECT @var_current_row,@var_total_rows,@var_filter_param,@var_product_code,@var_product_id,@var_channel,@var_channel_id,@var_table_name;

              WHILE (@var_filter_param != "") DO

                    SET @var_current_filter_param = SUBSTRING_INDEX(@var_filter_param,"&@&",1);
                    SET @var_dimension_id = SUBSTRING_INDEX(@var_current_filter_param,":",1);
                    SET @var_include_flag = SUBSTRING_INDEX(SUBSTRING_INDEX(@var_current_filter_param,":",2),":",-1);
                    SET @var_filter_value = SUBSTRING_INDEX(SUBSTRING_INDEX(@var_current_filter_param,":",3),":",-1);
                    
                    SELECT 1,/*SUBSTRING_INDEX(dimension_select_name,",",-1)*/ dimension_where_part INTO @var_count,@var_dimension_where_name
                    FROM od_qt_dimension_master 
                    WHERE dimension_id = @var_dimension_id AND FIND_IN_SET(@var_product_id,product_ids) > 0 AND IF(@var_channel_id > 0,FIND_IN_SET(@var_channel_id,channel_ids) > 0,1=1) AND active_status = 1;
      --               SELECT @var_count,@var_dimension_where_name;
                    ## To check whether the dimension_id is for analytics & diplay channel
                    IF (@var_count > 0) THEN
          
                        ## To get list of filtered accounts(used for getting partition list).
                        IF (@var_dimension_where_name = "account.account_id" AND @var_include_flag = 1) THEN
                        
                            SET @sql_get_account_ids = CONCAT("SELECT GROUP_CONCAT(@var_account_ids_list_include,account_id) INTO @var_account_ids_list_include 
                                                              FROM od_account_master 
                                                              
      --                                                          WHERE account_name LIKE '%",REPLACE(@var_filter_value,",",CONCAT("%' OR account_name LIKE '%")),"%'
                                                              WHERE FIND_IN_SET(account_id,@var_filter_value) > 0 AND active_status = 1
                                                              ");
      --                       SELECT @sql_get_account_ids;
                            PREPARE STMT FROM @sql_get_account_ids;
                            EXECUTE STMT;
                            DEALLOCATE PREPARE STMT;

                        ELSEIF (@var_dimension_where_name = "account.account_id" AND @var_include_flag = 0) THEN
                        
                            
                            SET @sql_get_account_ids = CONCAT("SELECT GROUP_CONCAT(@var_account_ids_list_exclude,account_id) INTO @var_account_ids_list_exclude
                                                              FROM od_account_master 
      --                                                          WHERE account_name LIKE '%",REPLACE(@var_filter_value,",",CONCAT("%' OR account_name LIKE '%")),"%'
                                                              WHERE FIND_IN_SET(account_id,@var_filter_value) > 0 AND active_status = 1
                                                              ");
                            
                            PREPARE STMT FROM @sql_get_account_ids;
                            EXECUTE STMT;
                            DEALLOCATE PREPARE STMT;
                        
                        END IF;
                        
                        IF (@var_dimension_where_name != "account.account_id") THEN
      --                       SET @where_cond = CONCAT(@where_cond," AND (",@var_dimension_where_name," ",IF(@var_include_flag = 1," = "," != "),"'",REPLACE(@var_filter_value,",",CONCAT("' ",IF(@var_include_flag = 1," OR "," AND ")," ",@var_dimension_where_name," ",IF(@var_include_flag = 1," = "," != "),"'")),"')" );
                            SET @where_cond = CONCAT(@where_cond," AND (",@var_dimension_where_name," ",IF(@var_include_flag = 1," IN "," NOT IN "),"('",REPLACE(@var_filter_value,",","','"),"'))" );
                        ELSEIF (@var_dimension_where_name = "account.account_id") THEN
                            SET @where_cond = CONCAT(@where_cond," AND (FIND_IN_SET(account.account_id,'",IF(@var_include_flag = 1,@var_account_ids_list_include,@var_account_ids_list_exclude),"') ",IF(@var_include_flag = 1," > "," = ")," 0 ) " );
                        END IF;
                        
                        IF(@var_dimension_where_name = "region.region_id") THEN
                            SET @var_dimension_check_HB_Region = 1;
                        END IF;
                        
                    END IF;
                    
                    SET @var_filter_param = REPLACE(@var_filter_param,CONCAT(@var_current_filter_param,"&@&"),"");
              END WHILE;
              
              SET @var_account_ids_list_include = TRIM(LEADING "," FROM @var_account_ids_list_include);
              SET @var_account_ids_list_exclude = TRIM(LEADING "," FROM @var_account_ids_list_exclude);
      --         SELECT @var_account_ids_list_include,@var_account_ids_list_exclude;
              SET @var_accountids_list_final = (SELECT IFNULL(GROUP_CONCAT(account_id),"") FROM od_account_master WHERE IF(IFNULL(@var_account_ids_list_include,"") != "", FIND_IN_SET(account_id,@var_account_ids_list_include) > 0,1=1) AND IF(IFNULL(@var_account_ids_list_exclude,"") != "",FIND_IN_SET(account_id,@var_account_ids_list_exclude) = 0,1=1) AND ignore_acc_processing_flag = 0 AND active_status = 1 );
              
              IF (@var_account_ids_list_include = "" AND @var_account_ids_list_exclude = "") THEN
                  SET @var_partion_name_list_main = (SELECT fn_od_select_partition_names_v4(@var_table_name,@var_start_day_id_loop,@var_end_day_id_loop));
                  SET @var_partion_name_list_adunit = (SELECT fn_od_select_partition_names_v4("od_account_dfp_daily_adunit_data",@var_start_day_id_loop,@var_end_day_id_loop));
                  SET @var_partion_name_list_crt = (SELECT fn_od_select_partition_names_v4("od_qt_daily_adunit_creative_data",@var_start_day_id_loop,@var_end_day_id_loop));
                  SET @var_partion_name_list_add = (SELECT fn_od_select_partition_names_v4("od_account_additional_data_report_header",@var_start_day_id_loop,@var_end_day_id_loop));
                  SET @var_partion_name_list_native_add = (SELECT fn_od_select_partition_names_v4("od_account_additional_native_data_report_header",@var_start_day_id_loop,@var_end_day_id_loop));
                  SET @var_partion_name_list_video_add = (SELECT fn_od_select_partition_names_v4("od_account_additional_video_data_report_header",@var_start_day_id_loop,@var_end_day_id_loop));
                  
                  SET @var_partion_name_list_ga = (SELECT fn_od_select_partition_names_v4("od_ga_account_site_viewid_report_data",@var_start_day_id_loop,@var_end_day_id_loop));
              ELSE
                  SET @var_partion_name_list_main = (SELECT fn_od_select_partition_names_v3(@var_table_name,@var_accountids_list_final,@var_start_day_id_loop,@var_end_day_id_loop));
                  SET @var_partion_name_list_adunit = (SELECT fn_od_select_partition_names_v3("od_account_dfp_daily_adunit_data",@var_accountids_list_final,@var_start_day_id_loop,@var_end_day_id_loop));
                  SET @var_partion_name_list_crt = (SELECT fn_od_select_partition_names_v3("od_qt_daily_adunit_creative_data",@var_accountids_list_final,@var_start_day_id_loop,@var_end_day_id_loop));
                  SET @var_partion_name_list_add = (SELECT fn_od_select_partition_names_v3("od_account_additional_data_report_header",@var_accountids_list_final,@var_start_day_id_loop,@var_end_day_id_loop));
                  SET @var_partion_name_list_native_add = (SELECT fn_od_select_partition_names_v3("od_account_additional_native_data_report_header",@var_accountids_list_final,@var_start_day_id_loop,@var_end_day_id_loop));
                  SET @var_partion_name_list_video_add = (SELECT fn_od_select_partition_names_v3("od_account_additional_video_data_report_header",@var_accountids_list_final,@var_start_day_id_loop,@var_end_day_id_loop));
                  
                  SET @var_partion_name_list_ga = (SELECT fn_od_select_partition_names_v3("od_ga_account_site_viewid_report_data",@var_accountids_list_final,@var_start_day_id_loop,@var_end_day_id_loop));
              END IF;
              
              
              -- ------------------------- SELECT ----------------------------
              
              
              SELECT IFNULL(GROUP_CONCAT(dimension_select_name SEPARATOR ","),"") INTO @var_dimension_select_main_part
              FROM od_qt_dimension_master
              WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND FIND_IN_SET(@var_product_id,product_ids) > 0 AND IF(@var_channel_id > 0,FIND_IN_SET(@var_channel_id,channel_ids) > 0,1=1) AND active_status = 1;
              
              ## Mid Select Part
              SELECT IFNULL(CONCAT("header.",REPLACE(GROUP_CONCAT(dimension_actual_entity_name SEPARATOR ","),",",",header.")),"") INTO @var_dimension_select_main_mid_part
              FROM od_qt_dimension_master
              WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND FIND_IN_SET(@var_product_id,product_ids) > 0 AND IF(@var_channel_id > 0,FIND_IN_SET(@var_channel_id,channel_ids) > 0,1=1) AND active_status = 1;
              
              ## Main Select Part
              SELECT GROUP_CONCAT(CONCAT(
                                            IF(
                                                FIND_IN_SET(@var_product_id,product_ids) > 0 
                                                AND 
                                                IF(@var_channel_id > 0,FIND_IN_SET(@var_channel_id,channel_ids) > 0,channel_ids = ""),
        --                                         CONCAT(metric_formula," AS ",metric_alias),CONCAT("'' AS ",metric_alias)
                                                CONCAT(IFNULL(extra_metrics,"")," ",metric_formula), CONCAT(fn_qt_get_extra_blanks(extra_metrics),", '' ")
                                                )
                                            ," AS ",metric_alias)
                                    ) 
                INTO @var_select_columns
              FROM od_qt_metric_master 
              WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND metric_formula IS NOT NULL AND active_status = 1
              ORDER BY metric_id;
              
              IF (@var_geo_device_flag = 1) THEN
--                   SET @var_select_columns = REPLACE(@var_select_columns,"IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',ROUND(SUM(5th_level_in_request),0)) AS 5th_level_in_request_display","IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',ROUND(SUM(IF(level = '5',in_request,0)),0)) AS 5th_level_in_request_display");
--                   
                  
                  SET @var_select_columns = REPLACE(@var_select_columns,
                  
                  "IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',IFNULL(SUM(IF(provider.provider_code = 'provider_affinity_hvr' OR adunit.sub_channel_id = 6,0,5th_level_in_request)),0)) AS 5th_level_in_request_display",
                  
                  "IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',
                  ROUND(SUM(IF(level = '5',IF(provider.provider_code = 'provider_affinity_hvr' OR adunit.sub_channel_id = 6,0,in_request),0)),0)) AS 5th_level_in_request_display");
                  
                  
              END IF;
              
              ## Mid Select Formula Part
              SELECT IFNULL(REPLACE(GROUP_CONCAT(CONCAT(
                                          IF(
                                              FIND_IN_SET(@var_product_id,product_ids) > 0 
                                              AND 
                                              IF(@var_channel_id > 0,FIND_IN_SET(@var_channel_id,channel_ids) > 0,channel_ids = ""),
                                              CONCAT(IFNULL(extra_mid_metrics,"")," ",mid_metric_formula), CONCAT(fn_qt_get_extra_blanks(extra_mid_metrics),", '' ")
                                            )
                                        ," AS ",metric_alias)
                                  ),"  AS "," AS "),"")
              INTO @var_select_mid_columns
              FROM od_qt_metric_master 
              WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND mid_metric_formula IS NOT NULL AND active_status = 1 
              AND od_column_flag NOT IN (2,3)
              ORDER BY metric_id;
              
              SET @var_select_mid_columns = IF(@var_select_mid_columns = "","'' AS temp_col",@var_select_mid_columns);

              
              IF((@var_partner_flag = 1 OR @var_neglect_unfilled_numbers = 1 OR arg_tab_ids IN (3,5)) AND FIND_IN_SET(5,arg_metric_ids) = 0 ) THEN
                  SET @var_select_mid_columns = REPLACE(@var_select_mid_columns,"SUM(unfilled_impressions_display)","SUM(0)");
--               ELSEIF(@var_neglect_unfilled_numbers = 1) THEN
--                   SET @var_select_mid_columns = REPLACE(@var_select_mid_columns,"SUM(unfilled_impressions_display)","SUM(0)");
              END IF;
              
              IF (@var_geo_device_flag = 1) THEN
--                   SET @var_select_mid_columns = REPLACE(@var_select_mid_columns,"IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',ROUND(SUM(5th_level_in_request),0)) AS 5th_level_in_request_display","IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',ROUND(SUM(IF(level = '5',in_request,0)),0)) AS 5th_level_in_request_display");
                  
                  
                  SET @var_select_mid_columns = REPLACE(@var_select_mid_columns,"IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',IFNULL(SUM(IF(provider.provider_code = 'provider_affinity_hvr' OR adunit.sub_channel_id = 6,0,5th_level_in_request)),0)) AS 5th_level_in_request_display",
                  
                  "IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',
                  ROUND(SUM(IF(level = '5',IF(provider.provider_code = 'provider_affinity_hvr' OR adunit.sub_channel_id = 6,0,in_request),0)),0)) AS 5th_level_in_request_display");
                  
              END IF;


              
              ## Mid GA Select Formula Part
              SELECT IFNULL(REPLACE(GROUP_CONCAT(CONCAT(
--                                           IF(
--                                               FIND_IN_SET(@var_product_id,product_ids) > 0 
--                                               AND 
--                                               IF(@var_channel_id > 0,FIND_IN_SET(@var_channel_id,channel_ids) > 0,channel_ids = ""),
                                              CONCAT(IFNULL(extra_mid_metrics,"")," ",IFNULL(mid_metric_formula,"''"))
--                                                 , CONCAT(fn_qt_get_extra_blanks(extra_mid_metrics),", '' ")
--                                             )
                                        ," AS ",metric_alias)
                                  ),"  AS "," AS "),"")
              INTO @var_select_mid_ga_columns
              FROM od_qt_metric_master 
              WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 /*AND mid_metric_formula IS NOT NULL*/ AND active_status = 1 
              AND od_column_flag IN (2,3)
              ORDER BY metric_id;


              
              ## Inner Select Columns 
              SELECT IFNULL(REPLACE(GROUP_CONCAT(CONCAT(
                                          IF(
                                              FIND_IN_SET(@var_product_id,product_ids) > 0 
                                              AND 
                                              IF(@var_channel_id > 0,FIND_IN_SET(@var_channel_id,channel_ids) > 0,channel_ids = ""),
                                              CONCAT(IFNULL(extra_metrics,"")," ",IFNULL(metric_formula,"''")),CONCAT(fn_qt_get_extra_blanks(extra_metrics),", '' ")
                                            )
                                        ," AS ",metric_alias)
                                  ),"  AS "," AS "),"") 
              INTO @var_select_columns_inner
              FROM od_qt_metric_master 
              WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 /*AND metric_formula IS NOT NULL*/ AND active_status = 1 
              AND metric_code NOT IN ("unfilled_impressions_display","total_active_view_per_viewable_impr_display","unfilled_impressions_app")
              ORDER BY metric_id;
              
              

--               ## Inner Select Columns 
--               SELECT IFNULL(REPLACE(GROUP_CONCAT(CONCAT(
--                                           IF(
--                                               FIND_IN_SET(@var_product_id,product_ids) > 0 
--                                               AND 
--                                               IF(@var_channel_id > 0,FIND_IN_SET(@var_channel_id,channel_ids) > 0,channel_ids = ""),
--                                               CONCAT(IFNULL(extra_metrics,"")," ",IFNULL(metric_formula,"''")),CONCAT(fn_qt_get_extra_blanks(extra_metrics),", '' ")
--                                             )
--                                         ," AS ",metric_alias)
--                                   ),"  AS "," AS "),"") 
--               INTO @var_select_columns_inner_ga
--               FROM od_qt_metric_master 
--               WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 /*AND metric_formula IS NOT NULL*/ AND active_status = 1 
--               AND metric_code NOT IN ("unfilled_impressions_display","total_active_view_per_viewable_impr_display","unfilled_impressions_app")
--               AND od_column_flag =2
--               ORDER BY metric_id;
                            
              
              IF (@var_geo_device_flag = 1) THEN
--                   SET @var_select_columns_inner = REPLACE(@var_select_columns_inner,"IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',SUM(5th_level_in_request)) AS 5th_level_in_request_display","IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',SUM(IF(level = '5',in_request,0))) AS 5th_level_in_request_display");
                  
                  SET @var_select_columns_inner = REPLACE(@var_select_columns_inner,"IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',IFNULL(SUM(IF(provider.provider_code = 'provider_affinity_hvr' OR adunit.sub_channel_id = 6,0,5th_level_in_request)),0)) AS 5th_level_in_request_display",
                  
                  "IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',
                  ROUND(SUM(IF(level = '5',IF(provider.provider_code = 'provider_affinity_hvr' OR adunit.sub_channel_id = 6,0,in_request),0)),0)) AS 5th_level_in_request_display");
                  
                  
              END IF;
              
              
              -- ------------------------- GROUP BY ----------------------------
              
              ## Main OR Inner Group BY
              SELECT IFNULL(GROUP_CONCAT(dimension_group_entity_name SEPARATOR ","),"") INTO @var_dimension_inner_group_by
              FROM od_qt_dimension_master
              WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND FIND_IN_SET(@var_product_id,product_ids) > 0 AND IF(@var_channel_id > 0,FIND_IN_SET(@var_channel_id,channel_ids) > 0,1=1) AND active_status = 1;
              
              ## Mid Group BY
              SELECT IFNULL(GROUP_CONCAT("header.",dimension_group_actual_entity_name SEPARATOR ","),"") INTO @var_extra_dimension_mid_group_by
              FROM od_qt_dimension_master
              WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND FIND_IN_SET(@var_product_id,product_ids) > 0 AND IF(@var_channel_id > 0,FIND_IN_SET(@var_channel_id,channel_ids) > 0,1=1) AND active_status = 1;
              
              -- -------------------------- OD -----------------------------
              
              ## For OD COLUMNS PART
              IF (@var_od_column_flag = 1) THEN 
              
                  SELECT (CASE 
                              WHEN GROUP_CONCAT(dimension_code ORDER BY 1) = 'publisher'      THEN 'monthly_status.account_id,'
                              WHEN GROUP_CONCAT(dimension_code ORDER BY 1) = 'site'           THEN 'monthly_status.site_id,'
                              WHEN GROUP_CONCAT(dimension_code ORDER BY 1) = 'publisher,site' THEN 'monthly_status.account_id,monthly_status.site_id,'
                              ELSE ''
                          END) AS select_payment_part,
                          (CASE 
                              WHEN GROUP_CONCAT(dimension_code ORDER BY 1) = 'publisher'      THEN 'data.account_id,'
                              WHEN GROUP_CONCAT(dimension_code ORDER BY 1) = 'site'           THEN 'data.site_id,'
                              WHEN GROUP_CONCAT(dimension_code ORDER BY 1) = 'publisher,site' THEN 'data.account_id,data.site_id,'
                              ELSE ''
                          END) AS select_mid_payment_part,
                          (CASE 
                              WHEN GROUP_CONCAT(dimension_code ORDER BY 1) = 'publisher'      THEN 'AND payment_data.account_id   = header.account_id'
                              WHEN GROUP_CONCAT(dimension_code ORDER BY 1) = 'site'           THEN 'AND payment_data.site_id      = site.site_id'
                              WHEN GROUP_CONCAT(dimension_code ORDER BY 1) = 'publisher,site' THEN 'AND payment_data.account_id   = header.account_id AND payment_data.site_id      = site.site_id'
                              ELSE ''
                          END) AS join_cond_payment_part
                  INTO @var_select_payment_part,@var_select_payment_mid_part,@var_join_cond_payment_part
                          
                  FROM od_qt_dimension_master
                  WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code IN ("publisher","site") AND active_status = 1;
                  
                  SET @where_modifed = IFNULL(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                        @where_cond,
                                        fn_qt_remove_filter_condition_part(@where_cond,"adunit.ad_unit_id"),""),
                                        fn_qt_remove_filter_condition_part(@where_cond,"provider.provider_display_name"),""),
                                        fn_qt_remove_filter_condition_part(@where_cond,"details.provider_au"),""),
                                        fn_qt_remove_filter_condition_part(@where_cond,"prov_category.provider_category_type_id"),""),
                                        fn_qt_remove_filter_condition_part(@where_cond,"geo.geo_id"),""),
                                        fn_qt_remove_filter_condition_part(@where_cond,"device_type.device_type_id"),""),
                                        fn_qt_remove_filter_condition_part(@where_cond,"bidder.bidder_id"),""),
                                        fn_qt_remove_filter_condition_part(@where_cond,"region.region_id"),""),"");
                                        
                  
                  SET @var_od_join = CONCAT(" 
                  
                                              LEFT JOIN 
                                              (
                                                  SELECT ",IF( @var_interval_type_code = 'cumulative','','data.year_id,data.month_id,'),"
                                                          ",@var_select_payment_mid_part," ",@var_channel_id," AS channel_id,
                                                          SUM(onedash_fees) AS onedash_fees,
                                                          SUM(adx_fees) AS adx_fees,
                                                          SUM(network_fees) AS network_fees,
                                                          SUM(direct_fees) AS direct_fees,
                                                          SUM(misc_fees) AS misc_fees,
                                                          benchmark_od
                                                  FROM (
                                                          SELECT  ",IF( @var_interval_type_code = 'cumulative','','monthly_status.year_id,monthly_status.month_id,'),"
                                                                  ",@var_select_payment_part," ",@var_channel_id," AS channel_id,

                                                                  sitewise_start_date,
                                                                  monthly_status.approve_status,

                                                                  IF(CONCAT(monthly_status.year_id,'-',monthly_status.month_id,'-01') < sitewise_start_date,'N/A',SUM(IF(monthly_status.approve_status = 0,IF(cache_data.module_type_id = 3 AND cache_data.sub_module_type_id = 2,0,cache_data.onedash_commission_fee * cur_api.currency_value * cur_api2.conversion_value),IF(payment_details.module_type_id = 3 AND payment_details.sub_module_type_id = 2,0,payment_details.onedash_commission_fee * cur_api.currency_value * cur_api2.conversion_value)))) AS onedash_fees,

                                                                  IF(CONCAT(monthly_status.year_id,'-',monthly_status.month_id,'-01') < sitewise_start_date,'N/A',SUM(IF(monthly_status.approve_status = 0,IF(cache_data.module_type_id = 3 AND cache_data.sub_module_type_id = 2,cache_data.onedash_commission_fee * cur_api.currency_value * cur_api2.conversion_value,0),IF(payment_details.module_type_id = 3 AND payment_details.sub_module_type_id = 2,payment_details.onedash_commission_fee * cur_api.currency_value * cur_api2.conversion_value,0)))) AS adx_fees,

                                                                  IF(CONCAT(monthly_status.year_id,'-',monthly_status.month_id,'-01') < sitewise_start_date,'N/A',SUM(IF(monthly_status.approve_status = 0,IF(cache_data.module_type_id = 1 AND cache_data.sub_module_type_id = 0,cache_data.onedash_commission_fee * cur_api.currency_value * cur_api2.conversion_value,0),IF(payment_details.module_type_id = 1 AND payment_details.sub_module_type_id = 0,payment_details.onedash_commission_fee * cur_api.currency_value * cur_api2.conversion_value,0)))) AS network_fees,

                                                                  IF(CONCAT(monthly_status.year_id,'-',monthly_status.month_id,'-01') < sitewise_start_date,'N/A',SUM(IF(monthly_status.approve_status = 0,IF(cache_data.module_type_id = 2 AND cache_data.sub_module_type_id = 0,cache_data.onedash_commission_fee * cur_api.currency_value * cur_api2.conversion_value,0),IF(payment_details.module_type_id = 2 AND payment_details.sub_module_type_id = 0,payment_details.onedash_commission_fee * cur_api.currency_value * cur_api2.conversion_value,0)))) AS direct_fees,

                                                                  IF(CONCAT(monthly_status.year_id,'-',monthly_status.month_id,'-01') < sitewise_start_date,'N/A',SUM(IF(monthly_status.approve_status = 0,IF(cache_data.module_type_id = 4 AND cache_data.sub_module_type_id = 0,cache_data.onedash_commission_fee * cur_api.currency_value * cur_api2.conversion_value,0),IF(payment_details.module_type_id = 4 AND payment_details.sub_module_type_id = 0,payment_details.onedash_commission_fee * cur_api.currency_value * cur_api2.conversion_value,0)))) AS misc_fees,
                                                                  
                                                                  IFNULL(benchmark_ecpm,0) AS benchmark_od
                                                                  
                                                          FROM od_account_payment_monthly_status monthly_status
                                                            
                                                          LEFT JOIN od_account_payment_cache_data cache_data 
                                                            ON  cache_data.year_id       = monthly_status.year_id
                                                            AND cache_data.month_id      = monthly_status.month_id
                                                            AND cache_data.account_id    = monthly_status.account_id
                                                            AND cache_data.site_id       = monthly_status.site_id
                                                            AND cache_data.channel_id    = ",@var_channel_id,"
                                                            
                                                            
                                                          LEFT JOIN od_account_payment_monthly_header payment_header
                                                            ON  payment_header.year_id       = monthly_status.year_id
                                                            AND payment_header.month_id      = monthly_status.month_id
                                                            AND payment_header.account_id    = monthly_status.account_id
                                                            AND payment_header.site_id       = monthly_status.site_id
                                                            AND payment_header.channel_id    = ",@var_channel_id,"
                                                            
                                                            
                                                          LEFT JOIN od_account_payment_monthly_details payment_details
                                                            ON payment_header.payment_monthly_header_id = payment_details.payment_monthly_header_id
                                                            AND cache_data.dfp_network_id = payment_details.dfp_network_id
                                                            AND cache_data.module_type_id = payment_details.module_type_id
                                                            AND cache_data.sub_module_type_id = payment_details.sub_module_type_id
                                                          
                                                          JOIN od_account_master account 
                                                              ON monthly_status.account_id = account.account_id
                                                              AND account.ignore_acc_processing_flag = 0 
              --                                                 AND account.active_status = 1
                                                          
                                                          JOIN od_account_site_master site
                                                              ON monthly_status.site_id = site.site_id 
              --                                                 AND site.active_status = 1
                                                          
                                                          JOIN od_account_site_channel_mapping site_chn_map 
                                                              ON monthly_status.account_id = site_chn_map.account_id 
                                                              AND monthly_status.site_id = site_chn_map.site_id 
                                                              AND site_chn_map.channel_id = ",@var_channel_id,"
              --                                                 AND site_chn_map.active_status = 1
                                                              
                                                          LEFT JOIN od_bm_account_site_details site_prop 
                                                              ON site_prop.site_id = site.site_id 
              --                                                 AND site_prop.active_status = 1
                                                              
                                                          LEFT JOIN (
                                                                      SELECT record_type,account_id,site_id,year_month_id,(benchmark_ecpm * cur_api.currency_value * cur_api2.conversion_value) AS benchmark_ecpm
                                                                      FROM od_monthly_benchmark_report_data benchmark
                                                                      LEFT JOIN od_system_currency_api_data cur_api  
                                                                          ON STR_TO_DATE(CONCAT(benchmark.year_month_id,'-01'),'%Y%m-%d') = cur_api.day_id  
                                                                          AND benchmark.currency_id  = cur_api.currency_id 
                                                                          
                                                                      LEFT JOIN od_system_currency_api_data cur_api2 
                                                                          ON STR_TO_DATE(CONCAT(benchmark.year_month_id,'-01'),'%Y%m-%d') = cur_api2.day_id 
                                                                          AND cur_api2.currency_id = ",arg_currency_id,"
                                                                      WHERE record_type = 'Total'
                                                                      AND '",arg_dimension_ids,"' IN ('1,2','2','2,1')
                                                                    ) benchmark
                                                            ON  benchmark.record_type   = 'Total'
                                                            AND benchmark.account_id    = monthly_status.account_id
                                                            AND benchmark.site_id       = monthly_status.site_id
                                                            AND IF(site_prop.comparison_type_id = 2,MONTH(STR_TO_DATE(benchmark.year_month_id,'%Y%m')) = MONTH(STR_TO_DATE(monthly_status.year_month_id,'%Y%m'))  ,1=1)
                                                          
                                                          LEFT JOIN (
                                                                    SELECT currency_id,CONVERT(CONCAT(YEAR(day_id),MONTH(day_id)) USING 'utf8') AS year_month_id,AVG(currency_value) AS currency_value
                                                                    FROM od_system_currency_api_data 
                                                                    WHERE currency_id = 1 /*Should be 1 always.To convert payment data in USD */
                                                                    GROUP BY CONCAT(YEAR(day_id),MONTH(day_id))
                                                                    ) cur_api
                                                                ON monthly_status.year_month_id = cur_api.year_month_id
                                                                
                                                          LEFT JOIN (
                                                                    SELECT currency_id,CONVERT(CONCAT(YEAR(day_id),MONTH(day_id)) USING 'utf8') AS year_month_id,AVG(conversion_value) AS conversion_value
                                                                    FROM od_system_currency_api_data 
                                                                    WHERE currency_id = ",arg_currency_id," /*To convert payment data FROM USD TO arg_currency_id */
                                                                    GROUP BY CONCAT(YEAR(day_id),MONTH(day_id))
                                                                    ) cur_api2 
                                                              ON monthly_status.year_month_id = cur_api2.year_month_id
                                                              
                                                          WHERE CONCAT(monthly_status.year_id,'-',monthly_status.month_id,'-01') BETWEEN DATE_SUB(DATE_ADD(LAST_DAY('",@var_start_day_id_loop,"'),INTERVAL 1 DAY),INTERVAL 1 MONTH) AND DATE_SUB(DATE_ADD(LAST_DAY('",@var_end_day_id_loop,"'),INTERVAL 1 DAY),INTERVAL 1 MONTH)
                                                          ",@where_modifed," 
                                                                
                                                          GROUP BY ",IF( @var_interval_type_code = 'cumulative','','monthly_status.year_id,monthly_status.month_id,'),"
                                                                  ",@var_select_payment_part," ,
                                                                  sitewise_start_date
                                                ) data
                                                GROUP BY ",IF( @var_interval_type_code = 'cumulative','','data.year_id,data.month_id,'),"
                                                          ",@var_select_payment_mid_part," 1
                                            ) payment_data 
                                              ON  1=1
                                              ",IF( @var_interval_type_code = 'cumulative','','
                                                    AND payment_data.year_id      = YEAR(header.day_id)
                                                    AND payment_data.month_id     = MONTH(header.day_id)
                                                  ')," ",@var_join_cond_payment_part,"
                                              AND payment_data.channel_id   = ",@var_channel_id,"
                                              
                                              
                                            ");
              END IF;
              
              -- ------------------------- JOIN ----------------------------
              
              SELECT IFNULL(GROUP_CONCAT(dimension_join SEPARATOR " "),"") INTO @var_extra_dimension_join
              FROM od_qt_dimension_master
              WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND FIND_IN_SET(@var_product_id,product_ids) > 0 AND IF(@var_channel_id > 0, FIND_IN_SET(@var_channel_id,channel_ids) > 0,1=1) AND active_status = 1;
              
              -- ---------------------------------- DISPLAY -------------------------------------------
              
              IF (@var_product_id = 1 AND @var_channel_id IN (1,6) ) THEN
                  
                  SET @var_is_adunit_present_flag = IF (
                                                        (SELECT COUNT(1) FROM od_qt_dimension_master WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code = "adunit") > 0 
                                                        OR
                                                        IF(LOCATE("&@&3:",arg_filter_param,1) > 0 OR SUBSTRING(arg_filter_param,1,2) = '3:',1,0) > 0
                                                        OR 
                                                        IF (FIND_IN_SET("display_richmedia",@var_tab_code) > 0 OR FIND_IN_SET("display_network",@var_tab_code) > 0,1,0) > 0,
                                                        1,0
                                                        );
                                                        
                  SET @var_extra_dimension_join_unf_view = "";
                  SELECT CONCAT(@var_extra_dimension_join_unf_view," JOIN od_account_master account 
                                                                      ON header.account_id = account.account_id 
                                                                      AND account.ignore_acc_processing_flag = 0 ")
                  INTO @var_extra_dimension_join_unf_view
                  FROM od_qt_dimension_master
                  WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code IN ("publisher") AND active_status = 1;
                  
                  
                  SELECT CONCAT(@var_extra_dimension_join_unf_view," JOIN od_account_site_master site 
                                                                      ON header.site_id = site.site_id  ")
                  INTO @var_extra_dimension_join_unf_view
                  FROM od_qt_dimension_master
                  WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code IN ("site") AND active_status = 1;
                  
                  SELECT CONCAT(@var_extra_dimension_join_unf_view," JOIN od_account_adunit_master adunit
                                                                      ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                                      AND adunit.exclude_flag = 0 ")
                  INTO @var_extra_dimension_join_unf_view
                  FROM od_qt_dimension_master
                  WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code IN ("adunit") AND active_status = 1;
                          
                  
                  #################### Calculate Unfilled Impressions ###########################
                  
                  IF (SELECT COUNT(1) FROM od_qt_metric_master WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND metric_code IN ("unfilled_impressions_display","dfp_impression_unfilled_display","unfilled_impressions_app",
                  "fillrate_display",
                  "ecpm_display",
                  "fillrate_app",
                  "ecpm_app",
                  "benchmark_comparison_per_display") AND active_status = 1) > 0 AND @var_partner_flag = 0 AND @var_neglect_unfilled_numbers = 0 THEN
                          
                        SELECT IFNULL(CONCAT(
                                      (CASE @var_interval_type_code
                                          WHEN "day" THEN " header.day_id = unfilled.day_id "
                                          WHEN "month" THEN " header.day_id = unfilled.day_id "
                                          WHEN "cumulative" THEN " header.day_id = unfilled.day_id "
                                      END),
                                      CONCAT(" AND ",GROUP_CONCAT(CONCAT("header.",SUBSTRING_INDEX(dimension_group_entity_name,".",-1)," = unfilled.",SUBSTRING_INDEX(dimension_group_entity_name,".",-1)) SEPARATOR " AND " )) 
                                      ),"") AS unfilled_join_on
                        INTO @var_unfilled_join_on
                        FROM od_qt_dimension_master
                        WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code IN ("publisher","site","adunit") AND active_status = 1;
            
                        SET @where_modifed = IFNULL(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                    @where_cond,
                                                    fn_qt_remove_filter_condition_part(@where_cond,"provider.provider_display_name"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"details.provider_au"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"prov_category.provider_category_type_id"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"geo.geo_id"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"device_type.device_type_id"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"bidder.bidder_id"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"region.region_id"),""),"");
                                                    
                                             
                        SET @unfilled_part = CONCAT("
                                                    ## Main Unfilled Calc
                                                    LEFT JOIN 
                                                    (
                                                          SELECT header.day_id, ",@var_dimension_select_main_part,",
                                                                  SUM(total_inventory_level_unfilled_impressions) AS unfilled_impressions_display,
                                                                  SUM(total_inventory_level_unfilled_impressions) AS unfilled_impressions_app
                                                          FROM od_account_dfp_daily_adunit_data ",@var_partion_name_list_adunit," header
                                                          
                                                          JOIN od_account_master account 
                                                              ON header.account_id = account.account_id 
                                                              AND account.ignore_acc_processing_flag = 0 
      --                                                         AND account.active_status = 1
                                                          
                                                          JOIN od_account_adunit_master adunit
                                                              ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                              AND adunit.exclude_flag = 0 
      --                                                         AND adunit.active_status = 1
                                                          
                                                          JOIN od_account_site_channel_mapping site_chn_map 
                                                              ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
      --                                                         AND site_chn_map.active_status = 1 
                                                          
                                                          JOIN od_account_site_master site 
                                                              ON site_chn_map.site_id = site.site_id 
      --                                                         AND site.active_status = 1
                                                        
                                                          ",@where_main," ",@where_modifed," ",@where_source_cond,"
                                                          ",@var_viewability_where,"
                                                          GROUP BY header.day_id, ",@var_dimension_inner_group_by,"
                                                      ) unfilled 
                                                      ON ",@var_unfilled_join_on,"");

                        
                        ## For Remaining Unfilled Impressions Part
      --                   IF (SELECT COUNT(1) FROM od_qt_dimension_master WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code = "adunit") > 0 THEN
                        IF (@var_is_adunit_present_flag = 1) THEN
                                SELECT REPLACE(CONCAT(",",GROUP_CONCAT(CONCAT(
                                                IF(
                                                    metric_code IN ("unfilled_impressions_display","dfp_impression_unfilled_display","unfilled_impressions_app"/*,
                                                                    "fillrate_display",
                                                                    "ecpm_display",
                                                                    "fillrate_app",
                                                                    "ecpm_app",
                                                                    "benchmark_comparison_per_display"*/),
      --                                               CONCAT(IFNULL(REPLACE(extra_mid_metrics,"5th_level_in_request_display_25","0"),"")," ",mid_metric_formula), CONCAT(fn_qt_get_extra_blanks(extra_mid_metrics),", '' ")
                                                    CONCAT(IFNULL(REPLACE(extra_mid_metrics,"5th_level_in_request_display_25","'0'"),"")," ",REPLACE(mid_metric_formula,"5th_level_in_request_display_25","0")), CONCAT(fn_qt_get_extra_blanks(extra_mid_metrics),", '' ")

                                                  )
                                              ," AS ",metric_alias)
                                        )),",,",",")
                                INTO @var_select_columns_unfilled
                                FROM od_qt_metric_master 
                                WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 /*AND metric_formula IS NOT NULL*/ AND active_status = 1
                                ORDER BY metric_id;
                                
                                IF(@var_benchmark_comp_flag = 1) THEN
                                    SET @var_select_columns_unfilled = CONCAT(@var_select_columns_unfilled,", """" AS display_overview_network_ecpm");
                                END IF;
                                
                                SET @remaining_unfilled_part = CONCAT(" 
                                
                                                                        UNION ALL
                                                                        ## Remaining Unfilled Calc
                                                                        SELECT ",@var_select_day_part," 
                                                                                ",@var_dimension_select_main_part,"  
                                                                                ",@var_select_columns_unfilled,"
                                                                        FROM (
                                                                                  SELECT header.day_id, 
                                                                                          ",@var_dimension_select_main_part,"     
                                                                                          ",@var_select_columns_unfilled,"
                                                                                  FROM 
                                                                                  (
                                                                                    SELECT data.account_id,data.day_id ,data.ad_unit_id,
                                                                                          SUM(total_inventory_level_unfilled_impressions) AS unfilled_impressions_display,
                                                                                          SUM(total_inventory_level_unfilled_impressions) AS unfilled_impressions_app 
                                                                                    FROM od_account_dfp_daily_adunit_data ",@var_partion_name_list_adunit," data
                                                                                    WHERE data.day_id BETWEEN '",@var_start_day_id_loop,"' AND '",@var_end_day_id_loop,"' 
                                                                                    GROUP BY data.account_id,data.day_id ,data.ad_unit_id 
                                                                                  ) header 
                                                                                  
                                                                                  JOIN od_account_master account 
                                                                                      ON header.account_id = account.account_id 
                                                                                      AND account.ignore_acc_processing_flag = 0 
                --                                                                       AND account.active_status = 1
                                                                                  
                                                                                  JOIN od_account_adunit_master adunit
                                                                                      ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                                                      AND adunit.exclude_flag = 0 
                --                                                                       AND adunit.active_status = 1
                                                                                  
                                                                                  JOIN od_account_site_channel_mapping site_chn_map 
                                                                                      ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
                --                                                                       AND site_chn_map.active_status = 1 
                                                                                  
                                                                                  JOIN od_account_site_master site 
                                                                                      ON site_chn_map.site_id = site.site_id 
                --                                                                       AND site.active_status = 1
                                                                                
                                                                                  LEFT JOIN od_qt_daily_traffic_data ",@var_partion_name_list_main," unfilled_null 
                                                                                  ON header.day_id = unfilled_null.day_id 
                                                                                  AND header.account_id = unfilled_null.account_id
                                                                                  AND header.ad_unit_id = unfilled_null.ad_unit_id
                                                                                  
                                                                                  AND FIND_IN_SET(unfilled_null.tab_id,'",arg_tab_ids,"') > 0 
                                                                                  
                                                                                  ",@where_main," ",@where_modifed," ",@where_source_cond,"
                                                                                  ",@var_viewability_where,"
          --                                                                         AND unfilled_null.day_id IS NULL
                                                                                  
                                                                                  GROUP BY header.day_id, ",@var_dimension_inner_group_by,"
                                                                                  HAVING SUM(unfilled_impressions_display) > 0
                                                                                  AND TRIM(BOTH ',' FROM GROUP_CONCAT(IFNULL(unfilled_null.day_id,''))) = ''
                                                                        ) header
                                                                        ",@var_extra_dimension_join_unf_view,"
                                                                                
                                                                        ",@var_group_by_day_part," ",@var_dimension_inner_group_by,"
                                                                        
                                                                        ");
                                                                        
                                SET @var_remaining_part = CONCAT(@var_remaining_part," ",@remaining_unfilled_part);
                        END IF;
                        
                  END IF;
                  
      --             SELECT @var_remaining_part;
                  
                  #################### Calculate Viewablity Per Impressions ###########################
                  IF (SELECT COUNT(1) FROM od_qt_metric_master WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND metric_code IN (
                  "adx_da_fillrate_display","adsense_da_fillrate_display","total_active_view_per_viewable_impr_display",
                  "adsense_impressions_display","adsense_revenue_display","adsense_average_ecpm_display",
                  "ad_exchange_impressions_display","ad_exchange_revenue_display","ad_exchange_average_ecpm_display") AND active_status = 1) > 0 THEN

                        SELECT IFNULL(CONCAT(
                                      (CASE @var_interval_type_code
                                          WHEN "day" THEN " header.day_id = view_per_data.day_id "
                                          WHEN "month" THEN " header.day_id = view_per_data.day_id "
                                          WHEN "cumulative" THEN " header.day_id = view_per_data.day_id "
                                      END),
                                      CONCAT(" AND ",GROUP_CONCAT(CONCAT("header.",SUBSTRING_INDEX(dimension_group_entity_name,".",-1)," = view_per_data.",SUBSTRING_INDEX(dimension_group_entity_name,".",-1)) SEPARATOR " AND " )) 
                                      ),"") AS unfilled_join_on
                        INTO @var_view_per_join_on
                        FROM od_qt_dimension_master
                        WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code IN ("publisher","site","adunit") AND active_status = 1;
                        
                        SET @where_modifed = IFNULL(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                                    @where_cond,
                                                    fn_qt_remove_filter_condition_part(@where_cond,"provider.provider_display_name"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"details.provider_au"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"prov_category.provider_category_type_id"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"geo.geo_id"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"device_type.device_type_id"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"bidder.bidder_id"),""),
                                                    fn_qt_remove_filter_condition_part(@where_cond,"region.region_id"),""),"");
                        
                        SET @viewable_part = CONCAT(" ## Viewablity Calc
                                                    LEFT JOIN (
                                                                SELECT header.day_id, ",@var_dimension_select_main_part," ,
                                                                        IFNULL(SUM(total_active_view_viewable_impressions),0) AS total_active_view_viewable_impressions_display,
                                                                        IFNULL(SUM(total_active_view_measurable_impressions),0) AS total_active_view_measurable_impressions_display,
                                                                        IFNULL(((SUM(total_active_view_viewable_impressions)/SUM(total_active_view_measurable_impressions) ) * 100),0) AS total_active_view_per_viewable_impr_display,
                                                                        IFNULL(SUM(adsense_line_item_level_impressions),0) AS adsense_line_item_level_impressions_display,
                                                                        IFNULL(SUM(adsense_line_item_level_revenue * cur_api.currency_value * cur_api2.conversion_value),0) AS adsense_line_item_level_revenue_display,
                                                                        IFNULL(SUM(ad_exchange_line_item_level_impressions),0) AS ad_exchange_line_item_level_impressions_display,
                                                                        IFNULL(SUM(ad_exchange_line_item_level_revenue * cur_api.currency_value * cur_api2.conversion_value),0) AS ad_exchange_line_item_level_revenue_display,
                                                                        IFNULL(SUM(total_impressions),0) AS total_impressions_display
                                                                        
                                                                FROM od_qt_daily_adunit_creative_data ",@var_partion_name_list_crt," header
                                                                /*
                                                                JOIN od_account_adv_au_order_lineitem_creative_mapping crt_map 
                                                                    ON header.account_id = crt_map.account_id 
                                                                    AND header.adv_au_order_lineitem_creative_mapping_id = crt_map.adv_au_order_lineitem_creative_mapping_id 
                                                                */
                                                                JOIN od_account_master account 
                                                                    ON header.account_id = account.account_id 
                                                                    AND account.ignore_acc_processing_flag = 0 
      --                                                               AND account.active_status = 1
                                                                
                                                                JOIN od_account_adunit_master adunit
                                                                    ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                                    AND adunit.exclude_flag = 0 
      --                                                               AND adunit.active_status = 1
                                                                
                                                                JOIN od_account_site_channel_mapping site_chn_map 
                                                                    ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
      --                                                               AND site_chn_map.active_status = 1 
                                                                
                                                                JOIN od_account_site_master site 
                                                                    ON site_chn_map.site_id = site.site_id 
      -- --                                                               AND site.active_status = 1
                                                                
                                                                LEFT JOIN od_system_currency_api_data cur_api  
                                                                    ON header.day_id = cur_api.day_id  
                                                                    AND header.currency_id  = cur_api.currency_id 
                                                                    
                                                                LEFT JOIN od_system_currency_api_data cur_api2 
                                                                    ON header.day_id = cur_api2.day_id 
                                                                    AND cur_api2.currency_id = ",arg_currency_id,"
                                                      
                                                              
                                                                ",@where_main," ",@where_modifed," ",@where_source_cond,"
                                                                 
                                                                ",@var_viewability_where," 
                                                                GROUP BY header.day_id, ",@var_dimension_inner_group_by,"
                                                      ) view_per_data
                                                      ON ",@var_view_per_join_on,"");                    
                  
                        ## For Remaining Viewability Impressions Part
      --                   IF (SELECT COUNT(1) FROM od_qt_dimension_master WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code = "adunit") > 0 THEN
                        IF (@var_is_adunit_present_flag = 1) THEN    
                                SELECT REPLACE(CONCAT(",",GROUP_CONCAT(CONCAT(
                                                IF(
      --                                               FIND_IN_SET("total_active_view_per_viewable_impr_display",metric_code) > 0,
                                                    metric_code IN ("adx_da_fillrate_display","adsense_da_fillrate_display","total_active_view_per_viewable_impr_display",
                                                                    "adsense_impressions_display","adsense_revenue_display","adsense_average_ecpm_display",
                                                                    "ad_exchange_impressions_display","ad_exchange_revenue_display","ad_exchange_average_ecpm_display"),
                                                    CONCAT(IFNULL(extra_mid_metrics,"")," ",mid_metric_formula), CONCAT(fn_qt_get_extra_blanks(extra_mid_metrics),", '' ")
                                                  )
                                              ," AS ",metric_alias)
                                        )),",,",",")
                                INTO @var_select_columns_viewability
                                FROM od_qt_metric_master 
                                WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 /*AND metric_formula IS NOT NULL*/ AND active_status = 1
                                ORDER BY metric_id;
                                
                                SELECT REPLACE(CONCAT(",",GROUP_CONCAT(CONCAT(
--                                                 IF(
--       --                                               FIND_IN_SET("total_active_view_per_viewable_impr_display",metric_code) > 0,
--                                                     metric_code IN ("adx_da_fillrate_display","adsense_da_fillrate_display","total_active_view_per_viewable_impr_display",
--                                                                     "adsense_impressions_display","adsense_revenue_display","adsense_average_ecpm_display",
--                                                                     "ad_exchange_impressions_display","ad_exchange_revenue_display","ad_exchange_average_ecpm_display"),
--                                                                     
--                                                                     CONCAT(IFNULL(extra_overall_metrics,"")," ",overall_metric_formula), 
--                                                                       IF(metric_code = "benchmark_comparison_per_display",
--                                                                           " '' AS revenue_display_79,'' AS ad_partner_impressions_display_79,'' AS 5th_level_in_request_display_79,'' AS display_ecpm_79 ",
--                                                                           CONCAT(fn_qt_get_extra_blanks(extra_overall_metrics),", '' ")
--                                                                         )
--                                                   )

                                                    ( CASE WHEN metric_code IN ("adx_da_fillrate_display","adsense_da_fillrate_display","total_active_view_per_viewable_impr_display",
                                                                    "adsense_impressions_display","adsense_revenue_display","adsense_average_ecpm_display",
                                                                    "ad_exchange_impressions_display","ad_exchange_revenue_display","ad_exchange_average_ecpm_display") THEN
                                                                    
                                                                    CONCAT(IFNULL(extra_overall_metrics,"")," ",overall_metric_formula)
                                                          WHEN metric_code IN ("benchmark_comparison_per_display") THEN
                                                                          " '' AS revenue_display_79,'' AS ad_partner_impressions_display_79,'' AS 5th_level_in_request_display_79,'' AS display_ecpm_79,'' "
                                                          ELSE CONCAT(fn_qt_get_extra_blanks(extra_overall_metrics),", '' ")
                                                        
                                                    END )


                                              ," AS ",metric_alias)
                                        )),",,",",")
                                INTO @var_select_overall_columns_viewability
                                FROM od_qt_metric_master 
                                WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 /*AND metric_formula IS NOT NULL*/ AND active_status = 1 AND metric_code != ""
                                ORDER BY metric_id;
                                
                                IF(@var_benchmark_comp_flag = 1) THEN
                                    SET @var_select_overall_columns_viewability = CONCAT(@var_select_overall_columns_viewability,", """" AS display_overview_network_ecpm");
                                END IF;
                                
                                
                                SET @remaining_viewability_part = CONCAT(" 
                                
                                                                        UNION ALL
                                                                        ## Remaining Viewability Calc
                                                                        SELECT ",@var_select_day_part," 
                                                                                ",@var_dimension_select_main_part,"  
                                                                                ",@var_select_overall_columns_viewability,"
                                                                        FROM (
                                                                                  SELECT header.day_id, 
                                                                                  ",@var_dimension_select_main_part,"  
                                                                                  ",@var_select_columns_viewability,"
                                                                                FROM 
                                                                                (
                                                                                  SELECT data.account_id,data.day_id,data.ad_unit_id,
                                                                                        IFNULL(SUM(total_active_view_viewable_impressions),0) AS total_active_view_viewable_impressions_display,
                                                                                        IFNULL(SUM(total_active_view_measurable_impressions),0) AS total_active_view_measurable_impressions_display,
                                                                                        IFNULL(((SUM(total_active_view_viewable_impressions)/SUM(total_active_view_measurable_impressions) ) * 100),0) AS total_active_view_per_viewable_impr_display,
                                                                                        IFNULL(SUM(adsense_line_item_level_impressions),0) AS adsense_line_item_level_impressions_display,
                                                                                        IFNULL(SUM(adsense_line_item_level_revenue * cur_api.currency_value * cur_api2.conversion_value),0) AS adsense_line_item_level_revenue_display,
                                                                                        IFNULL(SUM(ad_exchange_line_item_level_impressions),0) AS ad_exchange_line_item_level_impressions_display,
                                                                                        IFNULL(SUM(ad_exchange_line_item_level_revenue * cur_api.currency_value * cur_api2.conversion_value),0) AS ad_exchange_line_item_level_revenue_display,
                                                                                        IFNULL(SUM(total_impressions),0) AS total_impressions_display
                                                                                  
                                                                                  FROM od_qt_daily_adunit_creative_data ",@var_partion_name_list_crt," data
                                                                                  
                                                                                  /*JOIN od_account_adv_au_order_lineitem_creative_mapping crt_map 
                                                                                        ON data.account_id = crt_map.account_id 
                                                                                        AND data.adv_au_order_lineitem_creative_mapping_id = crt_map.adv_au_order_lineitem_creative_mapping_id 
                                                                                  */  
                                                                                  
                                                                                  LEFT JOIN od_system_currency_api_data cur_api  
                                                                                      ON data.day_id = cur_api.day_id  
                                                                                      AND data.currency_id  = cur_api.currency_id 
                                                                                      
                                                                                  LEFT JOIN od_system_currency_api_data cur_api2 
                                                                                      ON data.day_id = cur_api2.day_id 
                                                                                      AND cur_api2.currency_id = ",arg_currency_id,"
                                                                                  
                                                                                  WHERE data.day_id BETWEEN '",@var_start_day_id_loop,"' AND '",@var_end_day_id_loop,"' 
                                                                                  GROUP BY data.account_id,data.day_id,data.ad_unit_id
                                                                                ) header 
                                                                                JOIN od_account_master account 
                                                                                    ON header.account_id = account.account_id 
                                                                                    AND account.ignore_acc_processing_flag = 0 
              --                                                                       AND account.active_status = 1
                                                                                
                                                                                JOIN od_account_adunit_master adunit
                                                                                    ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                                                    AND adunit.exclude_flag = 0 
              --                                                                       AND adunit.active_status = 1
                                                                                
                                                                                JOIN od_account_site_channel_mapping site_chn_map 
                                                                                    ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
              --                                                                       AND site_chn_map.active_status = 1 
                                                                                
                                                                                JOIN od_account_site_master site 
                                                                                    ON site_chn_map.site_id = site.site_id 
              --                                                                       AND site.active_status = 1
                                                                                
                                                                                LEFT JOIN od_qt_daily_traffic_data ",@var_partion_name_list_main," unfilled_null 
                                                                                  ON header.day_id = unfilled_null.day_id 
                                                                                  AND header.account_id = unfilled_null.account_id
                                                                                  AND header.ad_unit_id = unfilled_null.ad_unit_id
                                                                                  
                                                                                  AND  FIND_IN_SET(unfilled_null.tab_id,'",arg_tab_ids,"') > 0
                                                                                  
                                                                                  
                                                                                ",@where_main," ",@where_modifed," ",@where_source_cond,"
                                                                                ",@var_viewability_where,"
      --                                                                         AND unfilled_null.day_id IS NULL
                                                                                  
                                                                                GROUP BY header.day_id, ",@var_dimension_inner_group_by,"
                                                                                HAVING TRIM(BOTH ',' FROM GROUP_CONCAT(IFNULL(unfilled_null.day_id,''))) = ''
                                                                            ) header
                                                                            ",@var_extra_dimension_join_unf_view,"
                                                                          
                                                                            ",@var_group_by_day_part," ",@var_dimension_inner_group_by,"
              
                                                                             ");
                                                                        
                                SET @var_remaining_part = CONCAT(@var_remaining_part," ",@remaining_viewability_part);
                        END IF;
                  END IF;
                
      --             SELECT @var_remaining_part;
                
              
                  IF (@var_geo_device_flag = 0) THEN
                  
                  
                            IF(@var_where_display ='' AND @var_where_app = '') THEN
                                SET @var_where_display = "1=1";
                                SET @var_where_app = "1=1";
                            ELSEIF(@var_where_display !='' AND @var_where_app = '') THEN
                                SET @var_where_app = "1=1";    
                            ELSEIF(@var_where_display ='' AND @var_where_app != '') THEN
                                SET @var_where_display = "1=1";    
                            END IF;
                            
                            SET @var_where_display_app = "";
                            SET @var_where_display_app = CONCAT("AND IF(",@var_channel_id," = 1,",@var_where_display,",",@var_where_app,")");
                                               
                            SET @var_display_overview_network_select = "";                   
                            IF(@var_benchmark_comp_flag = 1) THEN                    
                            
--                             SET @var_select_columns = CONCAT(@var_select_columns,',display_overview_network_ecpm');
                            SET @var_select_mid_columns = CONCAT(@var_select_mid_columns,',display_overview_network_ecpm');
                            
                                    SELECT IFNULL(CONCAT(
                                            (CASE @var_interval_type_code
                                                WHEN "day" THEN " header.day_id = display_overview_network.day_id "
                                                WHEN "month" THEN " header.day_id = display_overview_network.day_id "
                                                WHEN "cumulative" THEN " header.day_id = display_overview_network.day_id "
                                            END),
                                            CONCAT(" AND ",GROUP_CONCAT(CONCAT("header.",SUBSTRING_INDEX(dimension_group_entity_name,".",-1)," = display_overview_network.",SUBSTRING_INDEX(dimension_group_entity_name,".",-1)) SEPARATOR " AND " )) 
                                            ),"") AS display_overview_network_join_on
                                    INTO @var_display_overview_network_join_on
                                    FROM od_qt_dimension_master
                                    WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code IN ("publisher","site") AND active_status = 1;
                    
            
                                    SET @var_display_overview_network_select = CONCAT(" 
                                                                                     LEFT JOIN (   
                                                                                                SELECT  ",@var_select_day_part,",header.account_id,header.site_id,
                                                                                                IFNULL(SUM(revenue) * 1000 /  (SUM(5th_level_in_request) + IF(@var_partner_flag = 0 AND @var_neglect_unfilled_numbers = 0,SUM(unfilled_impressions_display),'N/A')),'N/A') AS display_overview_network_ecpm
                                                                                                FROM (
                                                                                                        SELECT header.day_id, account.account_id,account.account_name,site.site_id,site.site  , 
                                                                                                        IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',IFNULL(SUM(IF(provider.provider_code = 'provider_affinity_hvr' OR adunit.sub_channel_id = 6,0,5th_level_in_request)),0) ) AS 5th_level_in_request,
                                                                                                        IFNULL(SUM(IF(provider_category_type_name = 'Direct' AND direct.account_id IS NOT NULL,(ad_request * (direct_cur.currency_value * cur_api2.conversion_value * IFNULL(direct.ecpm,0) ) /1000),  (revenue * cur_api.currency_value * cur_api2.conversion_value))),0) AS revenue
                                                                                                        
                                                                                                        FROM od_account_payment_intermediate_data_report_header  ",@var_partion_name_list_main," header
                                                                                                        JOIN od_account_payment_intermediate_data_report_details ",@var_partion_name_list_main," details 
                                                                                                            ON header.account_data_report_header_id = details.account_data_report_header_id  
                                                                                                        
                                                                                                        JOIN od_account_master account 
                                                                                                            ON header.account_id = account.account_id 
                                                                                                            AND account.ignore_acc_processing_flag = 0 
                                                                                                --                                                   AND account.active_status = 1
                                                                                                        
                                                                                                        JOIN od_account_adunit_master adunit
                                                                                                            ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                                                                            AND adunit.exclude_flag = 0 
                                                                                                --                                                   AND adunit.active_status = 1
                                                                                                        
                                                                                                        JOIN od_account_site_channel_mapping site_chn_map 
                                                                                                            ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
                                                                                                --                                                   AND site_chn_map.active_status = 1 
                                                                                                        
                                                                                                        JOIN od_account_site_master site 
                                                                                                            ON site_chn_map.site_id = site.site_id 
                                                                                                --                                                   AND site.active_status = 1
                                                                                                        
                                                                                                        JOIN od_account_provider_master provider 
                                                                                                            ON details.provider_id = provider.provider_id 
                                                                                                --                                                   /*AND provider.active_status = 1*/
                                                                                                        
                                                                                                        JOIN od_provider_category_type_master prov_category 
                                                                                                            ON provider.provider_category_type_id = prov_category.provider_category_type_id
                                                                                                            
                                                                                                        LEFT JOIN od_provider_type_master provider_type 
                                                                                                            ON provider_type.provider_type_id = provider.provider_type_id
                                                                                                --                                                   AND provider_type.active_status = 1
                                                                                                        
                                                                                                        LEFT JOIN od_account_direct_provider_ecpm direct 
                                                                                                            ON provider.account_id = direct.account_id 
                                                                                                            AND provider.provider_id = direct.provider_id 
                                                                                                            AND header.day_id BETWEEN direct.start_day_id AND direct.end_day_id
                                                                                                        
                                                                                                        LEFT JOIN od_system_currency_api_data direct_cur   
                                                                                                            ON header.day_id = direct_cur.day_id 
                                                                                                            AND direct.direct_currency_id = direct_cur.currency_id
                                                                                                        
                                                                                                        LEFT JOIN od_system_currency_api_data cur_api  
                                                                                                            ON header.day_id = cur_api.day_id  
                                                                                                            AND details.currency_id  = cur_api.currency_id 
                                                                                                            
                                                                                                        LEFT JOIN od_system_currency_api_data cur_api2 
                                                                                                            ON header.day_id = cur_api2.day_id 
                                                                                                            AND cur_api2.currency_id = 1
                                                                                                        
                                                                                                        LEFT JOIN od_bm_account_site_details site_prop 
                                                                                                        ON site_prop.site_id = site.site_id 
                                                                                                            AND site_prop.active_status = 1

                                                                                                        ",@where_main,"   
                                                                                                        ",@where_cond," ",@where_source_cond,"
                                                                                                        ",@var_where_display_network_benchmark_comp,"
                                                                                                        
                                                                                                        AND (IFNULL(details.in_request,0)+IFNULL(details.ad_request,0)+IFNULL(details.ad_monetize,0)+IFNULL(details.revenue,0)+IFNULL(details.passback,0)) > 0
                                                                                                        
                                                                                                        GROUP BY header.day_id, account.account_id,site.site_id

                                                                                                ) header
                                                                                                
                                                                                                ## Main Unfilled Calc
                                                                                                LEFT JOIN 
                                                                                                (
                                                                                                        
                                                                                                        SELECT  unfilled_data.day_id, unfilled_data.account_id,unfilled_data.site_id,
                                                                                                                SUM(unfilled_impressions_display) AS unfilled_impressions_display,
                                                                                                                SUM(unfilled_impressions_app) AS unfilled_impressions_app
                                                                                                        FROM 
                                                                                                        (
                                                                                                            SELECT header.day_id, account.account_id,site.site_id,
                                                                                                                    SUM(total_inventory_level_unfilled_impressions) AS unfilled_impressions_display,
                                                                                                                    SUM(total_inventory_level_unfilled_impressions) AS unfilled_impressions_app
                                                                                                            FROM od_account_dfp_daily_adunit_data ",@var_partion_name_list_adunit," header
                                                                                                            
                                                                                                            JOIN od_account_master account 
                                                                                                                ON header.account_id = account.account_id 
                                                                                                                AND account.ignore_acc_processing_flag = 0 
                                                                                                    --                                                         AND account.active_status = 1
                                                                                                            
                                                                                                            JOIN od_account_adunit_master adunit
                                                                                                                ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                                                                                AND adunit.exclude_flag = 0 
                                                                                                    --                                                         AND adunit.active_status = 1
                                                                                                            
                                                                                                            JOIN od_account_site_channel_mapping site_chn_map 
                                                                                                                ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
                                                                                                    --                                                         AND site_chn_map.active_status = 1 
                                                                                                            
                                                                                                            JOIN od_account_site_master site 
                                                                                                                ON site_chn_map.site_id = site.site_id 
                                                                                                    --                                                         AND site.active_status = 1
                                                                                                        
                                                                                                            ",@where_main," ",@where_modifed," ",@where_source_cond,"
                                                                                                            ",@var_viewability_where,"
                                                                                                            GROUP BY header.day_id,account.account_id,site.site_id
                                                                                                            
                                                                                                            
                                                                                                            UNION ALL
                                                                                                            ## Remaining Unfilled Calc

                                                                                                            SELECT header.day_id,
                                                                                                                    account.account_id,site.site_id,      
                                                                                                                    SUM(unfilled_impressions_display) AS unfilled_impressions_display,
                                                                                                                    SUM(unfilled_impressions_app) AS unfilled_impressions_app
                                                                                                            FROM 
                                                                                                            (
                                                                                                            SELECT data.account_id,data.day_id ,data.ad_unit_id,
                                                                                                                    SUM(total_inventory_level_unfilled_impressions) AS unfilled_impressions_display,
                                                                                                                    SUM(total_inventory_level_unfilled_impressions) AS unfilled_impressions_app 
                                                                                                            FROM od_account_dfp_daily_adunit_data ",@var_partion_name_list_adunit," data
                                                                                                            WHERE data.day_id BETWEEN '",@var_start_day_id_loop,"' AND '",@var_end_day_id_loop,"' 
                                                                                                            GROUP BY data.account_id,data.day_id ,data.ad_unit_id 
                                                                                                            ) header 

                                                                                                            JOIN od_account_master account 
                                                                                                                ON header.account_id = account.account_id 
                                                                                                                AND account.ignore_acc_processing_flag = 0 
                                                                                                            --                                                                       AND account.active_status = 1

                                                                                                            JOIN od_account_adunit_master adunit
                                                                                                                ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                                                                                AND adunit.exclude_flag = 0 
                                                                                                            --                                                                       AND adunit.active_status = 1

                                                                                                            JOIN od_account_site_channel_mapping site_chn_map 
                                                                                                                ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
                                                                                                            --                                                                       AND site_chn_map.active_status = 1 

                                                                                                            JOIN od_account_site_master site 
                                                                                                                ON site_chn_map.site_id = site.site_id 
                                                                                                            --                                                                       AND site.active_status = 1

                                                                                                            LEFT JOIN od_qt_daily_traffic_data ",@var_partion_name_list_main," unfilled_null 
                                                                                                            ON header.day_id = unfilled_null.day_id 
                                                                                                            AND header.account_id = unfilled_null.account_id
                                                                                                            AND header.ad_unit_id = unfilled_null.ad_unit_id

                                                                                                            AND FIND_IN_SET(unfilled_null.tab_id,'",arg_tab_ids,"') > 0 

                                                                                                            ",@where_main," ",@where_modifed," ",@where_source_cond,"
                                                                                                            ",@var_viewability_where,"
                                                                                                            --                                                                         AND unfilled_null.day_id IS NULL

                                                                                                            GROUP BY header.day_id,account.account_id,site.site_id
                                                                                                            HAVING SUM(unfilled_impressions_display) > 0
                                                                                                            AND TRIM(BOTH ',' FROM GROUP_CONCAT(IFNULL(unfilled_null.day_id,''))) = ''
                                                                                                        ) unfilled_data
                                                                                                        GROUP BY unfilled_data.day_id, unfilled_data.account_id,unfilled_data.site_id
                                                                                                    ) unfilled 
                                                                                                    ON ",@var_unfilled_join_on,"
                                                                                                    ",@var_group_by_day_part,",header.account_id,header.site_id
                                                                                        )display_overview_network
                                                                                        
                                                                                        ON ",@var_display_overview_network_join_on," ");

                                            
/*                                    PREPARE STMT FROM @var_display_overview_network_select;
                                    EXECUTE STMT;
                                    DEALLOCATE PREPARE STMT; */                        
                            END IF;
--                   SELECT 1;
                            SET @var_join = CONCAT("
                                                    ## Display Calc
                                                    FROM (
                                                            SELECT header.day_id, ",@var_dimension_select_main_part," ",IF(@var_select_columns_inner != "", CONCAT(" , ",@var_select_columns_inner),""),"
                                    
                                                            FROM od_account_payment_intermediate_data_report_header ",@var_partion_name_list_main," header
                                                            JOIN od_account_payment_intermediate_data_report_details ",@var_partion_name_list_main," details 
                                                                ON header.account_data_report_header_id = details.account_data_report_header_id  
                                                            
                                                            JOIN od_account_master account 
                                                                ON header.account_id = account.account_id 
                                                                AND account.ignore_acc_processing_flag = 0 
            --                                                   AND account.active_status = 1
                                                            
                                                            JOIN od_account_adunit_master adunit
                                                                ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                                AND adunit.exclude_flag = 0 
            --                                                   AND adunit.active_status = 1
                                                            
                                                            JOIN od_account_site_channel_mapping site_chn_map 
                                                                ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
            --                                                   AND site_chn_map.active_status = 1 
                                                            
                                                            JOIN od_account_site_master site 
                                                                ON site_chn_map.site_id = site.site_id 
            --                                                   AND site.active_status = 1
                                                            
                                                            JOIN od_account_provider_master provider 
                                                                ON details.provider_id = provider.provider_id 
            --                                                   /*AND provider.active_status = 1*/
                                                            
                                                            JOIN od_provider_category_type_master prov_category 
                                                                ON provider.provider_category_type_id = prov_category.provider_category_type_id
                                                                
                                                            LEFT JOIN od_provider_type_master provider_type 
                                                                ON provider_type.provider_type_id = provider.provider_type_id
            --                                                   AND provider_type.active_status = 1
                                                            
                                                            LEFT JOIN od_account_direct_provider_ecpm direct 
                                                                ON provider.account_id = direct.account_id 
                                                                AND provider.provider_id = direct.provider_id 
                                                                AND header.day_id BETWEEN direct.start_day_id AND direct.end_day_id
                                                            
                                                            LEFT JOIN od_system_currency_api_data direct_cur   
                                                                ON header.day_id = direct_cur.day_id 
                                                                AND direct.direct_currency_id = direct_cur.currency_id
                                                            
                                                            LEFT JOIN od_system_currency_api_data cur_api  
                                                                ON header.day_id = cur_api.day_id  
                                                                AND details.currency_id  = cur_api.currency_id 
                                                                
                                                            LEFT JOIN od_system_currency_api_data cur_api2 
                                                                ON header.day_id = cur_api2.day_id 
                                                                AND cur_api2.currency_id = ",arg_currency_id,"
                                                            
                                                            LEFT JOIN od_bm_account_site_details site_prop 
                                                            ON site_prop.site_id = site.site_id 
                                                                AND site_prop.active_status = 1
                                                            
                                                            LEFT JOIN (
                                                                    SELECT record_type,account_id,site_id,year_month_id,(benchmark_ecpm * cur_api.currency_value * cur_api2.conversion_value) AS benchmark_od_79
                                                                    FROM od_monthly_benchmark_report_data benchmark
                                                                    LEFT JOIN od_system_currency_api_data cur_api  
                                                                        ON STR_TO_DATE(CONCAT(benchmark.year_month_id,'-01'),'%Y%m-%d') = cur_api.day_id  
                                                                        AND benchmark.currency_id  = cur_api.currency_id 
                                                                        
                                                                    LEFT JOIN od_system_currency_api_data cur_api2 
                                                                        ON STR_TO_DATE(CONCAT(benchmark.year_month_id,'-01'),'%Y%m-%d') = cur_api2.day_id 
                                                                        AND cur_api2.currency_id = ",arg_currency_id,"
                                                                    WHERE record_type = 'Total'
                                                                    AND '",arg_dimension_ids,"' IN ('1,2','2','2,1')
                                                                    )benchmark
                                                            ON  benchmark.record_type   = 'Total'
                                                            AND benchmark.account_id    = site.account_id
                                                            AND benchmark.site_id       = site.site_id
                                                            AND IF(site_prop.comparison_type_id = 2,MONTH(STR_TO_DATE(benchmark.year_month_id,'%Y%m')) = MONTH(header.day_id)  ,1=1)
                                                            
                                                            
                                                            ",@var_od_join,"
                                                            
                                                            ",@var_extra_dimension_join,"

                                                            ",@where_main," ",@where_cond," ",@where_source_cond,"
                                                            ",@var_where_display_app_overall,"
                                                            ",@var_where_display_app,"

                                                            AND (IFNULL(details.in_request,0)+IFNULL(details.ad_request,0)+IFNULL(details.ad_monetize,0)+IFNULL(details.revenue,0)+IFNULL(details.passback,0)) > 0
                                                            
                                                            GROUP BY header.day_id, ",@var_dimension_inner_group_by,"
                                                    
                                                    ) header
                                                    ",@var_display_overview_network_select,"
                                                    ",@unfilled_part,"
                                                    
                                                    ",@viewable_part,"
                                                    
                                                    ");
                    
                  ELSEIF (@var_geo_device_flag = 1) THEN
                  
                      SET @var_join = CONCAT("
                                              ## Display-Geo-Device Calc
                                              FROM (
                                                    SELECT header.day_id, ",@var_dimension_select_main_part,IF(@var_select_columns_inner != "", CONCAT(" , ",@var_select_columns_inner),""),"
                            
                                                    FROM od_account_additional_data_report_header ",@var_partion_name_list_add," header
                                                    JOIN od_account_additional_data_report_details ",@var_partion_name_list_add," details 
                                                        ON header.account_data_report_header_id = details.account_data_report_header_id  
                                                    
                                                    JOIN od_account_master account 
                                                        ON header.account_id = account.account_id 
                                                        AND account.ignore_acc_processing_flag = 0 
      --                                                   AND account.active_status = 1
                                                    
                                                    JOIN od_account_adunit_master adunit
                                                        ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                        AND adunit.exclude_flag = 0 
      --                                                   AND adunit.active_status = 1
                                                    
                                                    JOIN od_account_site_channel_mapping site_chn_map 
                                                        ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
      --                                                   AND site_chn_map.active_status = 1 
                                                    
                                                    JOIN od_account_site_master site 
                                                        ON site_chn_map.site_id = site.site_id 
      --                                                   AND site.active_status = 1
                                                    
                                                    JOIN od_account_provider_master provider 
                                                        ON details.provider_id = provider.provider_id 
      --                                                   AND provider.active_status = 1
                                                    
                                                    JOIN od_provider_category_type_master prov_category 
                                                        ON provider.provider_category_type_id = prov_category.provider_category_type_id
                                                        
                                                    LEFT JOIN od_provider_type_master provider_type 
                                                        ON provider_type.provider_type_id = provider.provider_type_id
      --                                                   AND provider_type.active_status = 1
                                                    
                                                    LEFT JOIN od_account_direct_provider_ecpm direct 
                                                        ON provider.account_id = direct.account_id 
                                                        AND provider.provider_id = direct.provider_id 
                                                        AND header.day_id BETWEEN direct.start_day_id AND direct.end_day_id
                                                    
                                                    LEFT JOIN od_system_currency_api_data direct_cur   
                                                        ON header.day_id = direct_cur.day_id 
                                                        AND direct.direct_currency_id = direct_cur.currency_id
                                                    
                                                    LEFT JOIN od_system_currency_api_data cur_api  
                                                        ON header.day_id = cur_api.day_id  
                                                        AND details.currency_id  = cur_api.currency_id 
                                                        
                                                    LEFT JOIN od_system_currency_api_data cur_api2 
                                                        ON header.day_id = cur_api2.day_id 
                                                        AND cur_api2.currency_id = ",arg_currency_id,"
                                                    
                                                    ",@var_extra_dimension_join,"
                                                    
                                                    ",@where_main," ",@where_cond," ",@where_source_cond,"
                                                    
                                                    ",@var_where_display_app_overall,"
                                                    
                                                    ",@var_where_display,"

                                                    AND (IFNULL(details.in_request,0)+IFNULL(details.ad_request,0)+IFNULL(details.ad_monetize,0)+IFNULL(details.revenue,0)+IFNULL(details.passback,0)) > 0
                                                    
                                                    GROUP BY header.day_id, ",@var_dimension_inner_group_by,"
                                              
                                              ) header
                                              
                                              ",@unfilled_part,"
                                              
                                              ",@viewable_part,"
                                              
                                              ");
                  END IF;
                  
              ELSEIF (@var_product_id = 1 AND @var_channel_id = 2) THEN
              
                  SET @var_native_extra_join = "";
                  SET @var_native_header_table = ""; 
                  SET @var_native_details_table = "";
                  SET @var_native_partition_list = "";
                  
                  IF(@var_geo_device_flag = 0) THEN 
                        
                        SET @var_native_header_table =  "od_account_native_data_report_header"; 
                        SET @var_native_details_table = "od_account_native_data_report_details";
                        SET @var_native_partition_list = @var_partion_name_list_main;
                        SET @var_native_extra_join = CONCAT(" JOIN od_account_adv_au_order_lineitem_creative_mapping crt_map 
                                                                ON header.account_id = crt_map.account_id 
                                                                AND header.adv_au_order_lineitem_creative_mapping_id = crt_map.adv_au_order_lineitem_creative_mapping_id
                                                              JOIN od_account_native_adunit_master adunit
                                                                ON crt_map.account_id = adunit.account_id AND crt_map.ad_unit_id = adunit.ad_unit_id 
      --                                                        AND adunit.active_status = 1  ");
                  
                  ELSE 
                        SET @var_native_header_table = "od_account_additional_native_data_report_header"; 
                        SET @var_native_details_table = "od_account_additional_native_data_report_details";
                        SET @var_native_partition_list = @var_partion_name_list_native_add;
                        SET @var_native_extra_join = CONCAT(" JOIN od_account_native_adunit_master adunit
                                                                ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
      --                                                        AND adunit.active_status = 1");
                  END IF;
                  
--                   SELECT 2;
                  
                  SET @var_join  = CONCAT("
                                          ## Native Calc
                                          FROM (
                                                    SELECT header.day_id, ",@var_dimension_select_main_part,IF(@var_select_columns_inner != "", CONCAT(" , ",@var_select_columns_inner),""),"
                                                    FROM ",@var_native_header_table," ",@var_native_partition_list," header
                                                    JOIN ",@var_native_details_table," ",@var_native_partition_list," details 
                                                        ON header.account_data_report_header_id = details.account_data_report_header_id
                                                    
                                                    ",@var_native_extra_join,"
                                                    
                                                    JOIN od_account_master account 
                                                        ON header.account_id = account.account_id AND account.ignore_acc_processing_flag = 0 
      --                                                   AND account.active_status = 1
                                                
                                                    
                                                    JOIN od_account_site_channel_mapping site_chn_map 
                                                        ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
      --                                                   AND site_chn_map.active_status = 1 
                                                    
                                                    JOIN od_account_site_master site 
                                                        ON site_chn_map.site_id = site.site_id 
      --                                                   AND site.active_status = 1
                                                    
                                                    JOIN od_account_provider_master provider 
                                                        ON /*header.account_id = provider.account_id 
                                                        AND*/ details.provider_id = provider.provider_id 
      --                                                   AND provider.active_status = 1
      
      
                                                    JOIN od_provider_category_type_master prov_category 
                                                        ON provider.provider_category_type_id = prov_category.provider_category_type_id      
      
      
                                                    LEFT JOIN od_system_currency_api_data cur_api  
                                                        ON header.day_id = cur_api.day_id 
                                                        AND details.currency_id  = cur_api.currency_id 
                                                    
                                                    LEFT JOIN od_system_currency_api_data cur_api2 
                                                        ON header.day_id = cur_api2.day_id 
                                                        AND cur_api2.currency_id = ",arg_currency_id,"
                                                    
                                                    ",@var_extra_dimension_join,"
                                                    
                                                    ",@var_od_join,"
                                                    
                                                    ",@where_main," ",@where_cond," ",@where_source_cond,"
                                                    ",@var_where_native_overall,"
                                                    ",@var_where_native,"

                                                    GROUP BY header.day_id, ",@var_dimension_inner_group_by,"
                                                ) header
                                              
                                          ");
                                          
                                          
              ELSEIF (@var_product_id = 1 AND @var_channel_id = 3) THEN
                     
                  SET @var_video_extra_join = "";
                  SET @var_video_header_table = ""; 
                  SET @var_video_details_table = "";
                  SET @var_video_partition_list = "";
                  
                  IF(@var_geo_device_flag = 0) THEN 
                        
                        SET @var_video_header_table =  "od_account_video_data_report_header"; 
                        SET @var_video_details_table = "od_account_video_data_report_details";
                        SET @var_video_partition_list = @var_partion_name_list_main;
                        SET @var_video_extra_join = CONCAT(" JOIN od_account_adv_au_order_lineitem_creative_mapping crt_map 
                                                                ON header.account_id = crt_map.account_id 
                                                                AND header.adv_au_order_lineitem_creative_mapping_id = crt_map.adv_au_order_lineitem_creative_mapping_id
                                                              JOIN od_account_video_adunit_master adunit
                                                                ON crt_map.account_id = adunit.account_id AND crt_map.ad_unit_id = adunit.ad_unit_id 
      --                                                        AND adunit.active_status = 1  ");
                  
                  ELSE 
                        SET @var_video_header_table = "od_account_additional_video_data_report_header"; 
                        SET @var_video_details_table = "od_account_additional_video_data_report_details";
                        SET @var_video_partition_list = @var_partion_name_list_video_add;
                        SET @var_video_extra_join = CONCAT(" JOIN od_account_video_adunit_master adunit
                                                                ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
      --                                                        AND adunit.active_status = 1");
                  END IF;
              
              
--                   SELECT 3;
                  
                  SET @var_join  = CONCAT("
                                          ## Video Calc
                                          FROM (
                                                    SELECT header.day_id, ",@var_dimension_select_main_part,IF(@var_select_columns_inner != "", CONCAT(" , ",@var_select_columns_inner),""),"
                                          
                                                    FROM ",@var_video_header_table,"  ",@var_video_partition_list," header
                                                    JOIN ",@var_video_details_table," ",@var_video_partition_list," details 
                                                        ON header.account_data_report_header_id = details.account_data_report_header_id
                                                    
                                                    ",@var_video_extra_join,"
                                                    
                                                    JOIN od_account_master account 
                                                        ON header.account_id = account.account_id 
                                                        AND account.ignore_acc_processing_flag = 0 
      --                                                   AND account.active_status = 1
                                                    
                                                    JOIN od_account_site_channel_mapping site_chn_map 
                                                        ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
      --                                                   AND site_chn_map.active_status = 1 
                                                    
                                                    JOIN od_account_site_master site 
                                                        ON site_chn_map.site_id = site.site_id 
      --                                                   AND site.active_status = 1
                                                        
                                                    JOIN od_account_provider_master provider 
                                                        ON /*header.account_id = provider.account_id 
                                                        AND*/ details.provider_id = provider.provider_id 
      --                                                   AND provider.active_status = 1
      
                                                    JOIN od_provider_category_type_master prov_category 
                                                        ON provider.provider_category_type_id = prov_category.provider_category_type_id      
                                                    
                                                    LEFT JOIN od_system_currency_api_data cur_api  
                                                        ON header.day_id = cur_api.day_id 
                                                        AND details.currency_id  = cur_api.currency_id 
                                                    
                                                    LEFT JOIN od_system_currency_api_data cur_api2 
                                                        ON header.day_id = cur_api2.day_id 
                                                        AND cur_api2.currency_id = ",arg_currency_id,"
                                                        
                                                    ",@var_od_join,"
                                                    
                                                    ",@var_extra_dimension_join,"
                                                    
                                                    ",@where_main," ",@where_cond," ",@where_source_cond,"
                                                    ",@var_where_video_overall,"
                                                    ",@var_where_video,"                                                    

                                                    GROUP BY header.day_id, ",@var_dimension_inner_group_by,"
                                                ) header
                                              
                                          ");
                                          
              ELSEIF (@var_product_id = 1 AND @var_channel_id = 5) THEN
               
--                   SELECT 4;
                  SET @var_join  = CONCAT("
                                          ## Youtube Calc
                                          FROM (
                                                    SELECT header.day_id, ",@var_dimension_select_main_part,IF(@var_select_columns_inner != "", CONCAT(" , ",@var_select_columns_inner),""),"
                                          
                                                    FROM od_account_youtube_data_report_header ",@var_partion_name_list_main," header
                                                    JOIN od_account_youtube_data_report_details ",@var_partion_name_list_main," details 
                                                        ON header.account_data_report_header_id = details.account_data_report_header_id
                                                    
                                                    JOIN od_account_master account 
                                                        ON header.account_id = account.account_id 
                                                        AND account.ignore_acc_processing_flag = 0 
      --                                                   AND account.active_status = 1
                                                    
                                                    JOIN od_account_youtube_video_master adunit
                                                        ON header.account_id = adunit.account_id 
                                                        AND header.video_id = adunit.video_id 
      --                                                   AND adunit.active_status = 1
                                                    
                                                    JOIN od_account_site_channel_mapping site_chn_map 
                                                        ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
      --                                                   AND site_chn_map.active_status = 1 
                                                    
                                                    JOIN od_account_site_master site 
                                                        ON site_chn_map.site_id = site.site_id 
      --                                                   AND site.active_status = 1
                                                        
                                                    
                                                    JOIN od_account_provider_master provider 
                                                        ON /*header.account_id = provider.account_id AND*/ details.provider_id = provider.provider_id 
      --                                                   AND provider.active_status = 1
      
                                                    JOIN od_provider_category_type_master prov_category 
                                                        ON provider.provider_category_type_id = prov_category.provider_category_type_id      
                                                    
                                                    LEFT JOIN od_system_currency_api_data cur_api  
                                                        ON header.day_id = cur_api.day_id 
                                                        AND details.currency_id  = cur_api.currency_id 
                                                    
                                                    LEFT JOIN od_system_currency_api_data cur_api2 
                                                        ON header.day_id = cur_api2.day_id 
                                                        AND cur_api2.currency_id = ",arg_currency_id,"
                                                        
                                                    ",@var_extra_dimension_join,"
                                                    
                                                    ",@where_main," ",@where_cond," 
                                                    ",@var_where_youtube_overall,"
                                                    ",@var_where_youtube,"                                                    

                                                    GROUP BY header.day_id, ",@var_dimension_inner_group_by,"
                                                ) header
                                              
                                          ");

              ELSEIF (@var_product_id = 2 ) THEN
                    
--              SELECT 5;         
                  SET @var_table_name_analytics_header = "";
                  SET @var_table_name_analytics_header  = IF(@var_dimension_check_HB_Region = 0, "od_account_hb_intermediate_data_report_header","od_account_hb_data_report_header");

                  SET @var_table_name_analytics_details = "";
                  SET @var_table_name_analytics_details = IF(@var_dimension_check_HB_Region = 0, "od_account_hb_intermediate_data_report_details","od_account_hb_data_report_details");  
                  
                  SET @var_hb_extra_join = "";
                  SET @var_hb_extra_join  = IF(@var_dimension_check_HB_Region = 1 AND @var_extra_dimension_join NOT LIKE '%hb_region_master%',"JOIN hb_region_master region
                                            ON region.region_id = header.region_id","");
                  
                  
                  SET @var_select_columns_inner = REPLACE(@var_select_columns_inner,"SUM","");
                  
                  
                  SET @var_join  = CONCAT("
                                          ## HB Calc
                                          FROM (
                                                    SELECT header.day_id, ",@var_dimension_select_main_part,IF(@var_select_columns_inner != "", CONCAT(" , ",@var_select_columns_inner),""),"
                                          
                                                    FROM ",@var_table_name_analytics_header,"  ",@var_partion_name_list_main," header
                                                    JOIN ",@var_table_name_analytics_details," ",@var_partion_name_list_main," details 
                                                        ON header.account_data_report_header_id = details.account_data_report_header_id
                                                    JOIN od_account_master account 
                                                        ON header.account_id = account.account_id AND account.ignore_acc_processing_flag = 0 
      --                                                   AND account.active_status = 1
                                                    
                                                    JOIN od_account_hb_adunit_master adunit 
                                                        ON header.account_id = adunit.account_id 
                                                        AND header.ad_unit_id  = adunit.ad_unit_id
      --                                                   AND adunit.active_status = 1
                                                        
                                                    JOIN od_account_site_master site 
                                                        ON header.site_id = site.site_id 
      --                                                   AND site.active_status = 1
      
                                    
      
      
                                                    JOIN od_account_provider_master provider 
                                                        ON /*header.account_id = provider.account_id AND*/ details.provider_id = provider.provider_id 
      --                                                   AND provider.active_status = 1
      
                                                    JOIN od_provider_category_type_master prov_category 
                                                        ON provider.provider_category_type_id = prov_category.provider_category_type_id  
                                                        
                                                
                                                    LEFT JOIN od_system_currency_api_data cur_api
                                                        ON header.day_id = cur_api.day_id 
                                                        AND details.currency_id  = cur_api.currency_id 
                                                    
                                                    LEFT JOIN od_system_currency_api_data cur_api2 
                                                        ON header.day_id = cur_api2.day_id 
                                                        AND cur_api2.currency_id = ",arg_currency_id,"
                                                    
                                                    ",@var_hb_extra_join,"
                                                    
                                                    ",@var_extra_dimension_join,"
                                                    
                                                    ",@where_main," ",@where_cond," ",@where_source_cond,"
                                                    
                                                 --   GROUP BY header.day_id, ",@var_dimension_inner_group_by,"
                                                ) header
                                          ");
                                          
              END IF;
              
              
     
              --  ---------------------------------------- GA ---------------------------------------------
             
              IF (@var_ga_column_flag = 1 /*AND @var_ga_count = 0*/) THEN   
--                     SELECT @var_ga_count;
--                     SET @var_ga_count = 1;
              
--                   SELECT @var_product_id,@var_channel_id;
--                   SELECT 7;
                  
                  IF (@var_product_id = 1 AND @var_channel_id = 1) THEN 
                      SET @sql_insert_revenue_display = CONCAT("  INSERT INTO od_tmp_daywise_rev(day_id,site_id,product_id,channel_id,revenue)
                                                                  SELECT  header.day_id,site.site_id,
                                                                          @var_product_id AS product_id,
                                                                          @var_channel_id AS channel_id,
                                                                      IFNULL(SUM(IF(provider_category_type_name = 'Direct' AND direct.account_id IS NOT NULL,
                                                                      (ad_request * (direct_cur.currency_value * cur_api2.conversion_value * IFNULL(direct.ecpm,0) ) /1000),  
                                                                      (revenue * cur_api.currency_value * cur_api2.conversion_value))),0) AS revenue
                                                                  FROM od_account_payment_intermediate_data_report_header ",@var_partion_name_list_main," header
                                                                  JOIN od_account_payment_intermediate_data_report_details ",@var_partion_name_list_main," details 
                                                                      ON header.account_data_report_header_id = details.account_data_report_header_id  
                                                                  
                                                                  JOIN od_account_master account 
                                                                      ON header.account_id = account.account_id 
                                                                      AND account.ignore_acc_processing_flag = 0 
                      --                                                   AND account.active_status = 1
                                                                  
                                                                  JOIN od_account_adunit_master adunit
                                                                      ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                                      AND adunit.exclude_flag = 0 
                      --                                                   AND adunit.active_status = 1
                                                                  
                                                                  JOIN od_account_site_channel_mapping site_chn_map 
                                                                      ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
                      --                                                   AND site_chn_map.active_status = 1 
                                                                  
                                                                  JOIN od_account_site_master site 
                                                                      ON site_chn_map.site_id = site.site_id 
                      --                                                   AND site.active_status = 1
                                                                  
                                                                  JOIN od_account_provider_master provider 
                                                                      ON details.provider_id = provider.provider_id 
                      --                                                   /*AND provider.active_status = 1*/
                                                                  
                                                                  JOIN od_provider_category_type_master prov_category 
                                                                      ON provider.provider_category_type_id = prov_category.provider_category_type_id
                                                                      
                                                                  LEFT JOIN od_provider_type_master provider_type 
                                                                      ON provider_type.provider_type_id = provider.provider_type_id
                      --                                                   AND provider_type.active_status = 1
                                                                  
                                                                  LEFT JOIN od_account_direct_provider_ecpm direct 
                                                                      ON provider.account_id = direct.account_id 
                                                                      AND provider.provider_id = direct.provider_id 
                                                                      AND header.day_id BETWEEN direct.start_day_id AND direct.end_day_id
                                                                  
                                                                  LEFT JOIN od_system_currency_api_data direct_cur   
                                                                      ON header.day_id = direct_cur.day_id 
                                                                      AND direct.direct_currency_id = direct_cur.currency_id
                                                                  
                                                                  LEFT JOIN od_system_currency_api_data cur_api  
                                                                      ON header.day_id = cur_api.day_id  
                                                                      AND details.currency_id  = cur_api.currency_id 
                                                                      
                                                                  LEFT JOIN od_system_currency_api_data cur_api2 
                                                                      ON header.day_id = cur_api2.day_id 
                                                                      AND cur_api2.currency_id = ",arg_currency_id,"
                                                                  
                                                                  
                                                                  ",@var_od_join,"
                                                                  
                                                                  ",@var_extra_dimension_join,"
                                                                  ",@where_main," ",@where_cond," ",@where_source_cond,"
                                                                  ",@var_where_display_app_overall,"
                                                                  AND site_chn_map.channel_id = 1
                                                                  AND (IFNULL(details.in_request,0)+IFNULL(details.ad_request,0)+IFNULL(details.ad_monetize,0)+IFNULL(details.revenue,0)+IFNULL(details.passback,0)) > 0
                                                                  GROUP BY header.day_id,site.site_id
                                                          ");
                                                          
                      PREPARE STMT FROM @sql_insert_revenue_display;
                      EXECUTE STMT;
                      DEALLOCATE PREPARE STMT;     
                  END IF;
              
--                   SELECT 8;
                  IF (@var_product_id = 1 AND @var_channel_id = 2) THEN                     
                      SET @sql_insert_revenue_native = CONCAT("   INSERT INTO od_tmp_daywise_rev(day_id,site_id,product_id,channel_id,revenue)
                                                                  SELECT header.day_id,site.site_id,@var_product_id AS product_id,
                                                                          @var_channel_id AS channel_id, 
                                                                          IFNULL(SUM(details.revenue * cur_api.currency_value * cur_api2.conversion_value),0) AS revenue
                                                                  FROM od_account_native_data_report_header ",@var_partion_name_list_main," header
                                                                  JOIN od_account_native_data_report_details ",@var_partion_name_list_main," details 
                                                                      ON header.account_data_report_header_id = details.account_data_report_header_id
                                                                  
                                                                  ",@var_native_extra_join,"
                                                                  
                                                                  JOIN od_account_master account 
                                                                      ON header.account_id = account.account_id AND account.ignore_acc_processing_flag = 0 
                  --                                                   AND account.active_status = 1
                                                              
                                                                  
                                                                  JOIN od_account_site_channel_mapping site_chn_map 
                                                                      ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
                  --                                                   AND site_chn_map.active_status = 1 
                                                                  
                                                                  JOIN od_account_site_master site 
                                                                      ON site_chn_map.site_id = site.site_id 
                  --                                                   AND site.active_status = 1
                                                                  
                                                                  JOIN od_account_provider_master provider 
                                                                      ON /*header.account_id = provider.account_id 
                                                                      AND*/ details.provider_id = provider.provider_id 
                  --                                                   AND provider.active_status = 1
                  
                  
                                                                  JOIN od_provider_category_type_master prov_category 
                                                                      ON provider.provider_category_type_id = prov_category.provider_category_type_id      
                  
                  
                                                                  LEFT JOIN od_system_currency_api_data cur_api  
                                                                      ON header.day_id = cur_api.day_id 
                                                                      AND details.currency_id  = cur_api.currency_id 
                                                                  
                                                                  LEFT JOIN od_system_currency_api_data cur_api2 
                                                                      ON header.day_id = cur_api2.day_id 
                                                                      AND cur_api2.currency_id = ",arg_currency_id,"
                                                                  
                                                                  ",@var_extra_dimension_join,"
                                                                  
                                                                  ",@var_od_join,"
                                                                  
                                                                  ",@where_main," ",@where_cond," ",@where_source_cond,"
                                                                  ",@var_where_native_overall,"
                                                                  GROUP BY header.day_id,site.site_id ");
                                                          
                      PREPARE STMT FROM @sql_insert_revenue_native;
                      EXECUTE STMT;
                      DEALLOCATE PREPARE STMT;
                  END IF;
              
--                   SELECT 9;                    
                  
                  IF (@var_product_id = 1 AND @var_channel_id = 3) THEN                     
                      SET @sql_insert_revenue_video = CONCAT("    INSERT INTO od_tmp_daywise_rev(day_id,site_id,product_id,channel_id,revenue)
                                                                  SELECT header.day_id,site.site_id,@var_product_id AS product_id,
                                                                          @var_channel_id AS channel_id,
                                                                          IFNULL(SUM(details.revenue * cur_api.currency_value * cur_api2.conversion_value),0) AS revenue
                                                                                      
                                                                  FROM od_account_video_data_report_header  ",@var_partion_name_list_main," header
                                                                  JOIN od_account_video_data_report_details ",@var_partion_name_list_main," details 
                                                                      ON header.account_data_report_header_id = details.account_data_report_header_id
                                                                  
                                                                  ",@var_video_extra_join,"
                                                                  
                                                                  JOIN od_account_master account 
                                                                      ON header.account_id = account.account_id 
                                                                      AND account.ignore_acc_processing_flag = 0 
                                              --                                                   AND account.active_status = 1
                                                                  
                                                                  JOIN od_account_site_channel_mapping site_chn_map 
                                                                      ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
                                              --                                                   AND site_chn_map.active_status = 1 
                                                                  
                                                                  JOIN od_account_site_master site 
                                                                      ON site_chn_map.site_id = site.site_id 
                                              --                                                   AND site.active_status = 1
                                                                      
                                                                  JOIN od_account_provider_master provider 
                                                                      ON /*header.account_id = provider.account_id 
                                                                      AND*/ details.provider_id = provider.provider_id 
                                              --                                                   AND provider.active_status = 1

                                                                  JOIN od_provider_category_type_master prov_category 
                                                                      ON provider.provider_category_type_id = prov_category.provider_category_type_id      
                                                                  
                                                                  LEFT JOIN od_system_currency_api_data cur_api  
                                                                      ON header.day_id = cur_api.day_id 
                                                                      AND details.currency_id  = cur_api.currency_id 
                                                                  
                                                                  LEFT JOIN od_system_currency_api_data cur_api2 
                                                                      ON header.day_id = cur_api2.day_id 
                                                                      AND cur_api2.currency_id = ",arg_currency_id,"
                                                                      
                                                                  ",@var_od_join,"
                                                                  
                                                                  ",@var_extra_dimension_join,"
                                                                  
                                                                  ",@where_main," ",@where_cond," ",@where_source_cond,"
                                                                  ",@var_where_video_overall,"
                                                                  GROUP BY header.day_id,site.site_id");
                                                          
                      PREPARE STMT FROM @sql_insert_revenue_video;
                      EXECUTE STMT;
                      DEALLOCATE PREPARE STMT;
                  END IF;
              
--                   SELECT 10;
                  
                  IF (@var_product_id = 1 AND @var_channel_id = 5) THEN                     
                  
                      SET @sql_insert_revenue_youtube = CONCAT("  INSERT INTO od_tmp_daywise_rev(day_id,site_id,product_id,channel_id,revenue)
                                                                  SELECT header.day_id,site.site_id,@var_product_id AS product_id,
                                                                          @var_channel_id AS channel_id,
                                                                          IFNULL(SUM(youtube_ad_revenue * cur_api.currency_value * cur_api2.conversion_value ),0) AS revenue
              
                                                                  FROM od_account_youtube_data_report_header ",@var_partion_name_list_main," header
                                                                  JOIN od_account_youtube_data_report_details ",@var_partion_name_list_main," details 
                                                                      ON header.account_data_report_header_id = details.account_data_report_header_id
                                                                  
                                                                  JOIN od_account_master account 
                                                                      ON header.account_id = account.account_id 
                                                                      AND account.ignore_acc_processing_flag = 0 
                                              --                                                   AND account.active_status = 1
                                                                  
                                                                  JOIN od_account_youtube_video_master adunit
                                                                      ON header.account_id = adunit.account_id 
                                                                      AND header.video_id = adunit.video_id 
                                              --                                                   AND adunit.active_status = 1
                                                                  
                                                                  JOIN od_account_site_channel_mapping site_chn_map 
                                                                      ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
                                              --                                                   AND site_chn_map.active_status = 1 
                                                                  
                                                                  JOIN od_account_site_master site 
                                                                      ON site_chn_map.site_id = site.site_id 
                                              --                                                   AND site.active_status = 1
                                                                      
                                                                  
                                                                  JOIN od_account_provider_master provider 
                                                                      ON /*header.account_id = provider.account_id AND*/ details.provider_id = provider.provider_id 
                                              --                                                   AND provider.active_status = 1

                                                                  JOIN od_provider_category_type_master prov_category 
                                                                      ON provider.provider_category_type_id = prov_category.provider_category_type_id      
                                                                  
                                                                  LEFT JOIN od_system_currency_api_data cur_api  
                                                                      ON header.day_id = cur_api.day_id 
                                                                      AND details.currency_id  = cur_api.currency_id 
                                                                  
                                                                  LEFT JOIN od_system_currency_api_data cur_api2 
                                                                      ON header.day_id = cur_api2.day_id 
                                                                      AND cur_api2.currency_id = ",arg_currency_id,"
                                                                      
                                                                  ",@var_extra_dimension_join,"
                                                                  
                                                                  ",@where_main," ",@where_cond," 
                                                                  ",@var_where_youtube_overall,"
                                                                  GROUP BY header.day_id,site.site_id ");
                                                          
                      PREPARE STMT FROM @sql_insert_revenue_youtube;
                      EXECUTE STMT;
                      DEALLOCATE PREPARE STMT;
                  END IF;
              
--                   SELECT 11;
                  
                  IF (@var_product_id = 1 AND @var_channel_id = 6) THEN 
                      SET @sql_insert_revenue_app = CONCAT("  INSERT INTO od_tmp_daywise_rev(day_id,site_id,product_id,channel_id,revenue)
                                                              SELECT  header.day_id,site.site_id,
                                                                      @var_product_id AS product_id,
                                                                      @var_channel_id AS channel_id,
                                                                      IFNULL(SUM(IF(provider_category_type_name = 'Direct' AND direct.account_id IS NOT NULL,
                                                                      (ad_request * (direct_cur.currency_value * cur_api2.conversion_value * IFNULL(direct.ecpm,0) ) /1000),  
                                                                      (revenue * cur_api.currency_value * cur_api2.conversion_value))),0) AS revenue
                                                              FROM od_account_payment_intermediate_data_report_header ",@var_partion_name_list_main," header
                                                              JOIN od_account_payment_intermediate_data_report_details ",@var_partion_name_list_main," details 
                                                                  ON header.account_data_report_header_id = details.account_data_report_header_id  
                                                              
                                                              JOIN od_account_master account 
                                                                  ON header.account_id = account.account_id 
                                                                  AND account.ignore_acc_processing_flag = 0 
                  --                                                   AND account.active_status = 1
                                                              
                                                              JOIN od_account_adunit_master adunit
                                                                  ON header.account_id = adunit.account_id AND header.ad_unit_id = adunit.ad_unit_id 
                                                                  AND adunit.exclude_flag = 0 
                  --                                                   AND adunit.active_status = 1
                                                              
                                                              JOIN od_account_site_channel_mapping site_chn_map 
                                                                  ON adunit.account_site_channel_mapping_id = site_chn_map.account_site_channel_mapping_id 
                  --                                                   AND site_chn_map.active_status = 1 
                                                              
                                                              JOIN od_account_site_master site 
                                                                  ON site_chn_map.site_id = site.site_id 
                  --                                                   AND site.active_status = 1
                                                              
                                                              JOIN od_account_provider_master provider 
                                                                  ON details.provider_id = provider.provider_id 
                  --                                                   /*AND provider.active_status = 1*/
                                                              
                                                              JOIN od_provider_category_type_master prov_category 
                                                                  ON provider.provider_category_type_id = prov_category.provider_category_type_id
                                                                  
                                                              LEFT JOIN od_provider_type_master provider_type 
                                                                  ON provider_type.provider_type_id = provider.provider_type_id
                  --                                                   AND provider_type.active_status = 1
                                                              
                                                              LEFT JOIN od_account_direct_provider_ecpm direct 
                                                                  ON provider.account_id = direct.account_id 
                                                                  AND provider.provider_id = direct.provider_id 
                                                                  AND header.day_id BETWEEN direct.start_day_id AND direct.end_day_id
                                                              
                                                              LEFT JOIN od_system_currency_api_data direct_cur   
                                                                  ON header.day_id = direct_cur.day_id 
                                                                  AND direct.direct_currency_id = direct_cur.currency_id
                                                              
                                                              LEFT JOIN od_system_currency_api_data cur_api  
                                                                  ON header.day_id = cur_api.day_id  
                                                                  AND details.currency_id  = cur_api.currency_id 
                                                                  
                                                              LEFT JOIN od_system_currency_api_data cur_api2 
                                                                  ON header.day_id = cur_api2.day_id 
                                                                  AND cur_api2.currency_id = ",arg_currency_id,"
                                                              
                                                              
                                                              ",@var_od_join,"
                                                              
                                                              ",@var_extra_dimension_join,"
                                                              ",@where_main," ",@where_cond," ",@where_source_cond,"
                                                              ",@var_where_display_app_overall,"
                                                              AND site_chn_map.channel_id = 6
                                                              AND (IFNULL(details.in_request,0)+IFNULL(details.ad_request,0)+IFNULL(details.ad_monetize,0)+IFNULL(details.revenue,0)+IFNULL(details.passback,0)) > 0
                                                              GROUP BY header.day_id,site.site_id");
                                                      
                      PREPARE STMT FROM @sql_insert_revenue_app;
                      EXECUTE STMT;
                      DEALLOCATE PREPARE STMT;
                  END IF;

                  DROP TEMPORARY TABLE IF EXISTS od_tmp_daywise_rev_final;
                  CREATE TEMPORARY TABLE od_tmp_daywise_rev_final
                  SELECT day_id,site_id,SUM(revenue) AS revenue
                  FROM od_tmp_daywise_rev
                  GROUP BY day_id,site_id;
                    
              END IF; 
                          
          
                

              -- -------------------------- Prepare Metadata --------------------
              
              INSERT INTO od_qt_prepare_report_metadata
                    (product_code                  ,product_id                  ,channel                  ,channel_id                  ,where_main                                                    ,where_condition                                               ,account_ids_list                       ,partion_name_list                       ,join_cond             ,select_interval_field                                                                        ,select_entity_part                                                                                                 ,select_cond                                                                                ,group_by_interval_field                                                                          ,group_by_cond                                                                                                  ,remaining_part)
              VALUES (IFNULL(@var_product_code,"")  ,IFNULL(@var_product_id,"")  ,IFNULL(@var_channel,"")  ,IFNULL(@var_channel_id,"")  ,IFNULL(/*IF(@var_channel_id IN (1,2,3,5,6),*/ ""/*,@where_main)*/ ,"")  ,IFNULL(/*IF(@var_channel_id IN (1,2,3,5,6),*/ ""/*,@where_cond)*/ ,"")  ,IFNULL(@var_accountids_list_final,"")  ,IFNULL(@var_partion_name_list_main,"")  ,IFNULL(@var_join,"")  ,IFNULL(/*IF(@var_channel_id IN (1,2,3,5,6),*/ @var_select_day_mid_part/*,@var_select_day_part)*/ ,"")  ,IFNULL(/*IF(@var_channel_id IN (1,2,3,5,6),*/ @var_dimension_select_main_mid_part/*,@var_dimension_select_main_part)*/ ,"")  ,IFNULL(/*IF(@var_channel_id IN (1,2,3,5,6),*/ @var_select_mid_columns/*,@var_select_columns)*/ ,"")  ,IFNULL(/*IF(@var_channel_id IN (1,2,3,5,6),*/ @var_group_by_day_mid_part/*,@var_group_by_day_part)*/ ,"")  ,IFNULL(/*IF(@var_channel_id IN (1,2,3,5,6),*/ @var_extra_dimension_mid_group_by/*,@var_dimension_inner_group_by)*/ ,"")  ,IFNULL(@var_remaining_part,""));
              
              #################################################


              SET @var_current_row = @var_current_row + 1;
          END WHILE;
          
          
          IF (@var_ga_column_flag = 1) THEN 
          
--               SELECT 21;
              SET @where_ga_cond = IFNULL(REPLACE(
                                            @where_cond,
                                            fn_qt_remove_filter_condition_part(@where_cond,"site_chn_map.channel_id"),""),"");
              
--               SELECT 22;
              
              
              SELECT IFNULL(CONCAT(" ON ",
                            (CASE @var_interval_type_code
                                WHEN "day" THEN " overall_data.day_id = ga_data.day_id "
                                WHEN "month" THEN " overall_data.year_id = ga_data.year_id AND overall_data.month_id = ga_data.month_id "
                                WHEN "cumulative" THEN " 1=1 "
                            END),
                            CONCAT(" AND ",GROUP_CONCAT(CONCAT("overall_data.",SUBSTRING_INDEX(dimension_group_entity_name,".",-1)," = ga_data.",SUBSTRING_INDEX(dimension_group_entity_name,".",-1)) SEPARATOR " AND " )) 
                            ),"") AS unfilled_join_on
              INTO @var_ga_join_on
              FROM od_qt_dimension_master
              WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND dimension_code IN ("publisher","site") AND active_status = 1;
              
                
              
              SET @var_join_ga  = CONCAT("
                                          ## GA Calc
                                          LEFT JOIN 
                                          (
                                              SELECT ",@var_select_day_part,"  ",@var_dimension_select_main_part,IF(@var_select_mid_ga_columns != "", CONCAT(" , ",@var_select_mid_ga_columns),""),"
                                      
                                              FROM 
                                              (
                                                  SELECT  header.day_id,header.account_id,header.site_id,
                                                          SUM(page_views) AS page_views_ga,
                                                          SUM(users) AS users_ga,
                                                          SUM(bounces) AS bounces_ga,
                                                          SUM(total_sessions) AS total_sessions_ga,
                                                          AVG(avg_session_duration) AS avg_time_spent_ga
                                                  FROM od_ga_account_site_viewid_report_data ",@var_partion_name_list_ga," header
                                                  GROUP BY header.day_id,header.account_id,header.site_id
                                                  
                                              ) header
                                              
                                              JOIN od_account_master account 
                                                      ON header.account_id = account.account_id AND account.ignore_acc_processing_flag = 0 
      --                                                   AND account.active_status = 1
                                                      
                                                  JOIN od_account_site_master site 
                                                      ON header.site_id = site.site_id 
                                                          AND site.active_status = 1
                                                  
                                                  
    --                                                   JOIN od_account_site_channel_mapping site_chn_map 
    --                                                       ON site.account_id = site_chn_map.account_id
    --                                                           AND site.site_id = site_chn_map.site_id
    --       --                                                  AND site_chn_map.active_status = 1 
                                                  
                                                  LEFT JOIN od_tmp_daywise_rev_final rev
                                                      ON header.day_id = rev.day_id
                                                      AND header.site_id = rev.site_id
    --                                                   ",@var_extra_dimension_join,"
                                                  
                                                  ",@where_main," 
                                                  ",@where_ga_cond,"
                                              
                                                  ",@var_group_by_day_part," ",@var_dimension_inner_group_by," 
                                          ) ga_data ",@var_ga_join_on);


              
--               SELECT 23;   
          END IF;
          -- ------------------------- SELECT OVERALL ----------------------------
              
          SELECT IFNULL(GROUP_CONCAT(overall_select_name SEPARATOR ","),"") INTO @var_dimension_select_main_overall
          FROM od_qt_dimension_master
          WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 
          AND active_status = 1;
          
          SELECT IFNULL(GROUP_CONCAT(REPLACE(dimension_actual_entity_name,"provider_display_name","provider_name") SEPARATOR ","),"") INTO @var_dimension_select_main_final
          FROM od_qt_dimension_master
          WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND active_status = 1;
          
          SELECT IFNULL(GROUP_CONCAT(CONCAT(extra_overall_metrics,overall_metric_formula)," AS ",metric_code SEPARATOR ","),"") INTO @var_dimension_select_formula_overall
          FROM od_qt_metric_master
          WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND active_status = 1;
          
          IF(@var_benchmark_comp_flag = 1) THEN
                SET @var_dimension_select_formula_overall = CONCAT(@var_dimension_select_formula_overall,' , display_overview_network_ecpm');
          END IF;
          
          SELECT IFNULL(GROUP_CONCAT(metric_code," AS ",metric_code SEPARATOR ","),"") INTO @var_dimension_select_formula_final
          FROM od_qt_metric_master
          WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND active_status = 1;
          
      --     SELECT IFNULL(GROUP_CONCAT(CONCAT(" SUM(",metric_code,") AS ",metric_code)),"") INTO @var_dimension_select_total_sum_formula
      --     FROM od_qt_metric_master
      --     WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND active_status = 1;

          SELECT IFNULL(GROUP_CONCAT(CONCAT(" SUM(",metric_code,") AS ",metric_code)),"") INTO @var_dimension_select_total_sum_formula
          FROM od_qt_metric_master
          WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND active_status = 1;
          
          SELECT IFNULL(GROUP_CONCAT( (CASE WHEN overall_metric_formula LIKE '%benchmark%' THEN "GROUP_CONCAT(DISTINCT 'N/A')" WHEN metric_code = "avg_time_spent_ga" THEN CONCAT("AVG(",overall_metric_formula,")") WHEN od_column_flag = 3 THEN CONCAT("SUM(",overall_metric_formula,")") ELSE overall_metric_formula END)," AS ",metric_code SEPARATOR ","),"") INTO @var_dimension_select_total_sum_formula_2
          FROM od_qt_metric_master
          WHERE FIND_IN_SET(metric_id,arg_metric_ids) > 0 AND active_status = 1;
          
          -- ------------------------- GROUP BY OVERALL ----------------------------
          
          
          SELECT IFNULL(GROUP_CONCAT(overall_group_entity_name SEPARATOR ","),"") INTO @var_dimension_group_by_overall
          FROM od_qt_dimension_master
          WHERE FIND_IN_SET(dimension_id,arg_dimension_ids) > 0 AND active_status = 1;
          
          
          
          ###################### Generate Inside Tables Start ############################
          
          SET @sql_create_inside_tables = "";
          SET @var_current_row1 = 1;
          SET @var_total_rows1 = "";
          
          OPEN cur_generate_inside_tables;
              SELECT FOUND_ROWS() INTO @var_total_rows1;
              
              WHILE (@var_current_row1 <= @var_total_rows1) DO 
      --             SELECT @var_current_row1,@var_total_rows1;
                  
                  FETCH cur_generate_inside_tables INTO cur_product_code,cur_product_id,cur_channel,cur_channel_id,cur_where_main,cur_where_condition,cur_account_ids_list,cur_partion_name_list,cur_join_cond,cur_select_interval_field,cur_select_entity_part,cur_select_cond,cur_group_by_interval_field,cur_group_by_cond,cur_remaining_part;
                  
      --             SELECT cur_product_code,cur_product_id,cur_channel,cur_channel_id,cur_where_main,cur_where_condition,cur_account_ids_list,cur_partion_name_list,cur_join_cond,cur_select_interval_field,cur_select_entity_part,cur_select_cond,cur_group_by_interval_field,cur_group_by_cond,cur_remaining_part;
                  
                  SET @sql_create_inside_tables = CONCAT(@sql_create_inside_tables," UNION ALL 
                                                          
                                                          SELECT ",cur_select_interval_field," ",cur_select_entity_part,",",cur_select_cond," 
                                                          ",cur_join_cond," 
                                                          ",cur_where_main," ",cur_where_condition,"
                                                          ",cur_group_by_interval_field," ",cur_group_by_cond,"
                                                          
                                                          ",cur_remaining_part,"
                                                          
                                                          ");
      --             SELECT @sql_create_inside_tables;
                  SET @var_current_row1 = @var_current_row1 + 1;
              END WHILE;
              
      --         SELECT @sql_create_inside_tables;
              
          CLOSE cur_generate_inside_tables;
          
          SET @sql_create_inside_tables = TRIM(LEADING " UNION ALL" FROM REPLACE(REPLACE(@sql_create_inside_tables,", ,",","),",,",","));
          
          SET @sql_create_inside_tables = CONCAT(
                                                  IF(@var_start_loop = 1," CREATE TEMPORARY TABLE od_qt_final_data_temp AS "," INSERT INTO od_qt_final_data_temp ")," 
                                                  
                                                  SELECT ",@var_select_day_overall," 
                                                         ",@var_dimension_select_main_overall,",
                                                         ",@var_dimension_select_formula_overall,"
                                                         
                                                    FROM 
                                                    ( 
                                                        ",@sql_create_inside_tables," 
                                                    ) overall_data 
                                                    ",@var_join_ga,"

                                                    ",@var_group_by_day_overall," 
                                                    ",@var_dimension_group_by_overall 
                                                );
          
          PREPARE STMT FROM @sql_create_inside_tables;
          EXECUTE STMT;
          DEALLOCATE PREPARE STMT;

          
          
          SET @var_start_loop = @var_start_loop + 1;
    END WHILE;



    ################################################################################
    
    SET @sql_final_select = CONCAT("SELECT ",@var_select_day_final," ",@var_dimension_select_main_final,",",@var_dimension_select_formula_final," FROM od_qt_final_data_temp overall_data");
    
    PREPARE STMT FROM @sql_final_select;
    EXECUTE STMT;
    DEALLOCATE PREPARE STMT;
    
    SET @sql_total_summation_rr = CONCAT("SELECT ",@var_dimension_select_total_sum_formula_2,"
    FROM od_qt_final_data_temp");
    
    PREPARE STMT FROM @sql_total_summation_rr;
    EXECUTE STMT;
    DEALLOCATE PREPARE STMT;
    
    
END;


 CALL sp_od_qt_select_report_data_v2_ritu_1(9,'1','1','1','2021-10-01','2021-10-31','2','1:1:28&@&2:1:29','1,2','6,8,9,79',1,'account.account_name ASC','0','10','1'); -- 402
 CALL sp_od_qt_select_report_data_v2_ritu_1(9,'1','1','1','2021-10-01','2021-10-31','1','1:1:28&@&2:1:29','1,2','6,8,9,79',1,'account.account_name ASC','0','10','1'); -- 402

 CALL sp_od_qt_select_report_data_v2_ritu_1(9,'1','1','2','2021-05-01','2021-11-30','2','1:1:24&@&2:1:26','1,2','6,8,9,79,70',1,'account.account_name ASC','0','10','1');

 CALL sp_od_qt_select_report_data_v2(9,'1','1','2','2021-05-01','2021-11-30','2','1:1:24&@&2:1:26','1,2','6,8,9,79,70',1,'account.account_name ASC','0','10','1');

 -- SELECT @sql_create_inside_tables\G
