## DIY Reports -- 

-- Tables 
-- od_qt_source_type_master
-- od_qt_dimension_master
-- od_qt_metric_master

-- SPS

-- sp_od_qt_list_source_type_master
-- sp_od_qt_list_dimensions
-- sp_od_qt_list_metric

-- Functions

-- fn_od_select_partition_names_v3 -- Can pass multiple accounts
-- fn_od_select_partition_names_v4 -- No Need to Pass AccountID

####################################################################################
DROP TABLE IF EXISTS od_qt_source_type_master;
CREATE TABLE od_qt_source_type_master(
source_type_id INT(11) UNSIGNED PRIMARY KEY AUTO_INCREMENT,
source_type_name VARCHAR(255),
active_status TINYINT(1) UNSIGNED DEFAULT 1,
creation_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
updation_date TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO od_qt_source_type_master(source_type_name) VALUES ("ALL"),("DFP"),("NON-DFP");

####################################################################################

DROP TABLE IF EXISTS od_qt_dimension_master;
CREATE TABLE od_qt_dimension_master (
  dimension_id INT(11) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
  dimension_name VARCHAR(255) NOT NULL,
  dimension_code VARCHAR(255) NOT NULL,
  product_ids VARCHAR(255) NOT NULL,
  channel_ids VARCHAR(255) DEFAULT "",
  source_type_ids VARCHAR(255) DEFAULT "",
  partner_flag TINYINT(1) UNSIGNED,
  dimension_select_name VARCHAR(255),
  dimension_join MEDIUMTEXT,
  dimension_group_entity_name VARCHAR(255),
  overall_select_name VARCHAR(255),
  overall_group_entity_name VARCHAR(255),
  active_status TINYINT(1) UNSIGNED DEFAULT 1,
  creation_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updation_date TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_key_dimension(dimension_code)
  ) ENGINE=MyISAM DEFAULT CHARSET=latin1;


INSERT INTO od_qt_dimension_master
(dimension_name,dimension_code,product_ids,channel_ids,source_type_ids ,partner_flag ,dimension_select_name,overall_select_name,dimension_group_entity_name,overall_group_entity_name,dimension_join) values
("Publisher",   "publisher",    "1,2,3",  "1,2,3,5,6",  "2,3"          ,0             ,"account.account_id,account.account_name"                                           , "a.account_id,a.account_name"                                       ,"account.account_id"                      , "a.account_id"                ," "),
("Site",        "site",         "1,2,3",  "1,2,3,5,6",  "2,3"          ,0             ,"site.site_id,site.site"                                                            , "a.site_id,a.site"                                                  ,"site.site_id"                            , "a.site_id"                   ," "),
("Adunit",      "adunit",       "1,2",    "1,2,3,6",    "2"            ,0             ,"adunit.ad_unit_id,adunit.adunit_name"                                              , "a.ad_unit_id,a.adunit_name"                                        ,"adunit.ad_unit_id"                       , "a.ad_unit_id"                ,""),
("Adpartner",   "ad_partner",   "1",      "1,2,3,6",    "2,3"          ,1             ,"provider.provider_id,provider.provider_display_name"                               , "a.provider_id,a.provider_display_name AS provider_name"            ,"provider.provider_id"                    , "a.provider_id"               ," "),
("TagName",     "tag_name",     "1",      "1,2,3,6",    "2,3"          ,1             ,"details.provider_au"                                                               , "a.provider_au"                                                     ,"details.provider_au"                     , "a.provider_au"               ,""),
("PartnerType", "partner_type", "1",      "1,6",        "2,3"          ,1             ,"prov_category.provider_category_type_id,prov_category.provider_category_type_name" , "a.provider_category_type_id,a.provider_category_type_name"         ,"prov_category.provider_category_type_id" , "a.provider_category_type_id" ,""),
("Geo",         "geo",          "1,2",    "1",          "2"            ,0             ,"geo.geo_id,geo.geo_name"                                                           , "a.geo_id,a.geo_name"                                               ,"geo.geo_id"                              , "a.geo_id"                    ," JOIN od_account_geo_master geo ON header.geo_id = geo.geo_id AND geo.active_status = 1"),
("Device-Type", "device_type",  "1",      "1",          "2"            ,0             ,"device_type.device_type_id,device_type.device_type_name"                           , "a.device_type_id,a.device_type_name"                               ,"device_type.device_type_id"              , "a.device_type_id"            ," JOIN od_account_device_type_master device_type ON header.device_type_id = device_type.device_type_id AND device_type.active_status = 1"),
("Bidder",      "bidder",       "2",      "",           ""             ,0             ,"bidder.bidder_id,bidder.bidder_name"                                               , "a.bidder_id,a.bidder_name"                                         ,"bidder.bidder_id"                        , "a.bidder_id"                 ," JOIN hb_bidder_master bidder ON header.bidder_id = bidder.bidder_id AND bidder.active_status = 1"),
("WP-Audience", "wp_audience",  "3",      "",           ""             ,0             ,""                                                                                  , ""                                                                  ,""                                        , ""                            ,""),
("WP-Campign",  "wp_campaign",  "3",      "",           ""             ,0             ,""                                                                                  , ""                                                                  ,""                                        , ""                            ,""),
("WP-Source",   "wp_source",    "3",      "",           ""             ,0             ,""                                                                                  , ""                                                                  ,""                                        , ""                            ,""),
("WP-Status",   "wp_status",    "3",      "",           ""             ,0             ,""                                                                                  , ""                                                                  ,""                                        , ""                            ,"");



####################################################################################

DROP TABLE IF EXISTS od_qt_metric_master;
CREATE TABLE od_qt_metric_master(
metric_id int(11) unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
metric_name VARCHAR(255) NOT NULL,
metric_code VARCHAR(255) NOT NULL,
product_ids VARCHAR(255) NOT NULL,
channel_ids VARCHAR(255) DEFAULT NULL,
source_type_ids VARCHAR(255) DEFAULT NULL,
partner_flag TINYINT(1) UNSIGNED DEFAULT '0',
metric_alias VARCHAR(255) ,
metric_formula VARCHAR(512) ,
overall_metric_formula VARCHAR(512) ,
metric_comment VARCHAR(512) DEFAULT NULL,
applicable_dimension_ids VARCHAR(512) /*For Od Columns (Bechmark : 1 OR Pub Revenue : 1,2)  */,
active_status TINYINT(1) UNSIGNED DEFAULT 1,
creation_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
updation_date TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
UNIQUE KEY uq_key_metric(metric_code)
);


INSERT INTO od_qt_metric_master 
(metric_name,metric_code                                 ,product_ids,channel_ids,source_type_ids/*,partner_flag*/            ,metric_alias                                  ,overall_metric_formula                                                                                                                                                                                                                         ,metric_formula,metric_comment,applicable_dimension_ids) VALUES 
("DFP Impression"                          ,"dfp_impression_display"                             ,"1"  ,"1"   ,"2"    /*,0*/  ,"5th_level_in_request_display"                ,"IF(GROUP_CONCAT(DISTINCT 5th_level_in_request_display) = 'N/A','N/A',SUM(5th_level_in_request_display)) AS 5th_level_in_request_display"                                                                                                      ,"IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',ROUND(SUM(5th_level_in_request),0)) AS 5th_level_in_request_display"                                                                                                                                    ,"",""),
("Ad Request"                              ,"ad_request_display"                                 ,"1"  ,"1"   ,"2,3"  /*,0*/  ,"ad_request_display"                          ,"IF(GROUP_CONCAT(DISTINCT ad_request_display) = 'N/A','N/A',SUM(ad_request_display)) AS ad_request_display"                                                                                                                                    ,"IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',ROUND(SUM(in_request),0)) AS ad_request_display"                                                                                                                                                        ,"",""),
("Ad Partner Impression"                   ,"ad_partner_impressions_display"                     ,"1"  ,"1"   ,"2,3"  /*,1*/  ,"ad_partner_impressions_display"              ,"SUM(ad_partner_impressions_display) AS ad_partner_impressions_display"                                                                                                                                                                        ,"ROUND(SUM( ad_request  ),0) AS ad_partner_impressions_display"                                                                                                                                                                                                                         ,"",""),
("Paid Impression"                         ,"paid_impressions_display"                           ,"1"  ,"1"   ,"2,3"  /*,1*/  ,"paid_impressions_display"                    ,"SUM(paid_impressions_display) AS paid_impressions_display"                                                                                                                                                                                    ,"ROUND(SUM(ad_monetize),0) AS paid_impressions_display"                                                                                                                                                                                                                                 ,"",""),
("Unfilled Impression"                     ,"unfilled_impressions_display"                       ,"1"  ,"1"   ,"2"    /*,0*/  ,"unfilled_impressions_display"                ,"IF(@var_partner_flag = 0,SUM(unfilled_impressions_display),'N/A') AS unfilled_impressions_display"                                                                                                                                            ,"IF(@var_partner_flag = 0,IFNULL(ROUND(SUM(unfilled_impressions),0),0),0) AS unfilled_impressions_display"                                                                                                                                                                              ,"",""),
("Fill Rate"                               ,"fillrate_display"                                   ,"1"  ,"1"   ,"2,3"  /*,1*/  ,"fillrate_display"                            ,"IF(@var_partner_flag = 1,ROUND(IFNULL(SUM(paid_impressions_display)/SUM(ad_partner_impressions_display) * 100,'N/A'),2),ROUND(IFNULL(SUM(paid_impressions_display)/SUM(5th_level_in_request_display) * 100,'N/A'),2)) AS fillrate_display"    ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("CPM"                                     ,"cpm_display"                                        ,"1"  ,"1"   ,"2,3"  /*,1*/  ,"cpm_display"                                 ,"IFNULL(ROUND(SUM(revenue_display) * 1000 / SUM(paid_impressions_display),2),'N/A') AS cpm_display"                                                                                                                                            ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("Revenue"                                 ,"revenue_display"                                    ,"1"  ,"1"   ,"2,3"  /*,1*/  ,"revenue_display"                             ,"IFNULL(ROUND(SUM(revenue_display),2),0) AS revenue_display"                                                                                                                                                                                   ,"ROUND(IFNULL(IF(provider_category_type_name = 'Direct' AND direct.account_id IS NOT NULL,SUM(ad_request * (direct_cur.currency_value*IFNULL(direct.ecpm,0) ) /1000),  SUM(revenue * cur_api.currency_value * cur_api2.conversion_value)),0),2) AS revenue_display"                     ,"",""),
("eCPM"                                    ,"ecpm_display"                                       ,"1"  ,"1"   ,"2,3"  /*,0*/  ,"ecpm_display"                                ,"IF(@var_partner_flag = 1,ROUND(IFNULL(SUM(revenue_display) * 1000 /  SUM(ad_partner_impressions_display),'N/A'),2),ROUND(IFNULL(SUM(revenue_display) * 1000 /  SUM(5th_level_in_request_display),'N/A'),2)) AS ecpm_display"                  ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("Effective Fill Rate DFP"                 ,"effective_fillrate_dfp_display"                     ,"1"  ,"1"   ,"2"    /*,0*/  ,"effective_fillrate_dfp_display"              ,"IF(@var_partner_flag = 0,IFNULL(ROUND(SUM(paid_impressions_display)/SUM(5th_level_in_request_display) * 100,2),'N/A'),'N/A') AS effective_fillrate_dfp_display"                                                                               ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("Effective Fill Rate AdPartner"           ,"effective_fillrate_ad_partner_display"              ,"1"  ,"1"   ,"2,3"  /*,1*/  ,"effective_fillrate_ad_partner_display"       ,"IF(@var_partner_flag = 1,IFNULL(ROUND(SUM(paid_impressions_display)/SUM(ad_request_display) * 100,2),'N/A'),'N/A') AS effective_fillrate_ad_partner_display"                                                                                  ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("Direct imps"                             ,"direct_imps_display"                                ,"1"  ,"1"   ,"2"    /*,0*/  ,"direct_imps_display"                         ,"IFNULL(ROUND(SUM(direct_imps_display),0),0) AS direct_imps_display"                                                                                                                                                                           ,"IFNULL(ROUND(SUM(IF(prov_category.provider_category_type_name = 'Direct',ad_monetize,0)),0),0) AS direct_imps_display"                                                                                                                                                                 ,"",""),
("Direct Revenue"                          ,"direct_revenue_display"                             ,"1"  ,"1"   ,"2"    /*,0*/  ,"direct_revenue_display"                      ,"IFNULL(ROUND(SUM(direct_revenue_display),0),0) AS direct_revenue_display"                                                                                                                                                                     ,"IFNULL(ROUND(SUM(IF(prov_category.provider_category_type_name = 'Direct',revenue,0)),2),0) AS direct_revenue_display"                                                                                                                                                                  ,"",""),
("Network imps"                            ,"network_imps_display"                               ,"1"  ,"1"   ,"2"    /*,0*/  ,"network_imps_display"                        ,"IFNULL(ROUND(SUM(network_imps_display),0),0) AS network_imps_display"                                                                                                                                                                         ,"IFNULL(ROUND(SUM(IF(prov_category.provider_category_type_name = 'Direct' OR adunit.ad_unit_id < 0,0,ad_monetize)),0),0) AS network_imps_display"                                                                                                                                       ,"",""),
("Network Monetised Imps"                  ,"network_monetised_imps_display"                     ,"1"  ,"1"   ,"2"    /*,0*/  ,"network_monetised_imps_display"              ,"IFNULL(ROUND(SUM(network_monetised_imps_display),0),0) AS network_monetised_imps_display"                                                                                                                                                     ,"IFNULL(ROUND(SUM(IF(prov_category.provider_category_type_name = 'Direct' OR adunit.ad_unit_id < 0,0,ad_monetize)),0),0) AS network_monetised_imps_display"                                                                                                                             ,"",""),
("Network Rev"                             ,"network_revenue_display"                            ,"1"  ,"1"   ,"2"    /*,0*/  ,"network_revenue_display"                     ,"IFNULL(ROUND(SUM(network_revenue_display),0),0) AS network_revenue_display"                                                                                                                                                                   ,"IFNULL(ROUND(SUM(IF(prov_category.provider_category_type_name = 'Direct' OR adunit.ad_unit_id < 0,0,revenue)),2),0) AS network_revenue_display"                                                                                                                                        ,"",""),
("ADX DA fill rate"                        ,"adx_da_fillrate_display"                            ,"1"  ,"1"   ,"2"    /*,0*/  ,"adx_da_fillrate_display"                     ,"IF(@var_partner_flag = 0,IFNULL(ROUND(SUM(IF(provider_type.provider_type_name = 'provider_adx',5th_level_in_request_display,0) / SUM(5th_level_in_request_display)),2),0),'N/A') AS adx_da_fillrate_display"                                  ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("Adsense DA fill rate"                    ,"adsense_da_fillrate_display"                        ,"1"  ,"1"   ,"2"    /*,0*/  ,"adsense_da_fillrate_display"                 ,"IF(@var_partner_flag = 0,IFNULL(ROUND(SUM(IF(provider_type.provider_type_name = 'provider_adsense',5th_level_in_request_display,0)) / SUM(5th_level_in_request_display),2),0),'N/A') AS adsense_da_fillrate_display"                          ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("eCPM on DFP"                             ,"ecpm_dfp_display"                                   ,"1"  ,"1"   ,"2"    /*,0*/  ,"ecpm_dfp_display"                            ,"IF(@var_partner_flag = 0,IFNULL(ROUND(SUM(revenue_display) * 1000 /  SUM(5th_level_in_request_display),2),0),'N/A') AS ecpm_dfp_display"                                                                                                      ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("eCPM for on AP Req"                      ,"ecpm_ad_partner_display"                            ,"1"  ,"1"   ,"2,3"  /*,1*/  ,"ecpm_ad_partner_display"                     ,"IFNULL(ROUND(SUM(revenue_display) * 1000 /  SUM(ad_partner_impressions_display),2),0) AS ecpm_ad_partner_display"                                                                                                                             ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("eFill Rate on DFP"                       ,"efillrate_dfp_display"                              ,"1"  ,"1"   ,"2"    /*,0*/  ,"efillrate_dfp_display"                       ,"IF(@var_partner_flag = 0,IFNULL(ROUND(SUM(paid_impressions_display) /  SUM(5th_level_in_request_display),2),0),'N/A') AS efillrate_dfp_display"                                                                                               ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("eFill Rate on AP Req"                    ,"efillrate_ad_partner_display"                       ,"1"  ,"1"   ,"2,3"  /*,1*/  ,"efillrate_ad_partner_display"                ,"IFNULL(ROUND(SUM(paid_impressions_display) /  SUM(ad_partner_impressions_display),2),0) AS efillrate_ad_partner_display"                                                                                                                      ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("Clicks"                                  ,"clicks_display"                                     ,"1"  ,"1"   ,"2"            ,"clicks_display"                              ,"IFNULL(ROUND(SUM(clicks_display),0),0) AS clicks_display"                                                                                                                                                                                     ,"IFNULL(ROUND(SUM(clicks),0),0) AS clicks_display"                                                                                                                                                                                                                                      ,"",""),
("CTR"                                     ,"ctr_display"                                        ,"1"  ,"1"   ,"2"            ,"clicks_display"                              ,"IFNULL(ROUND(SUM(clicks_display) / SUM(5th_level_in_request_display),2),0) AS ctr_display"                                                                                                                                                    ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("DFP Impressions + Unfilled"              ,"dfp_impression_unfilled_display"                    ,"1"  ,"1"   ,"2"    /*,0*/  ,"dfp_impression_unfilled_display"             ,"IF(@var_partner_flag = 0,ROUND(SUM(5th_level_in_request_display + unfilled_impressions_display),0),'N/A') AS dfp_impression_unfilled_display"                                                                                                 ,NULL                                                                                                                                                                                                                                                                                    ,"",""),
("Total Active View % viewable impressions","total_active_view_per_viewable_impr_display"        ,"1"  ,"1"   ,"2"            ,"total_active_view_per_viewable_impr_display" ,"IF(@var_partner_flag = 0,ROUND(SUM(total_active_view_per_viewable_impr_display),0),'N/A') AS total_active_view_per_viewable_impr_display"                                                                                                     ,"IF(@var_partner_flag = 0,IFNULL(ROUND(SUM(total_active_view_per_viewable_impr_display),0),0),'N/A') AS total_active_view_per_viewable_impr_display"                                                                                                                                    ,"",""),


INSERT INTO od_qt_metric_master
(metric_name,metric_code,product_ids,channel_ids,source_type_ids/*,partner_flag*/,metric_alias           ,overall_metric_formula                                                            ,metric_formula,metric_comment,applicable_dimension_ids) VALUES
("DFP Impression"       ,"dfp_impression_native"    ,"1"   ,"2"  ,"2"   /*,0*/  ,"dfp_impression_native" ,"SUM(dfp_impression_native) AS dfp_impression_native"                             ,"IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',ROUND(SUM(IF(@var_partner_flag = 1, dfp_impressions,IF( level = 5, dfp_impressions ,0 ))),0)) AS dfp_impression_native"   ,"",""),
("Ad Partner Impression","requests_native"          ,"1"   ,"2"  ,"2,3" /*,1*/  ,"requests_native"       ,"SUM(requests_native) AS requests_native"                                         ,"ROUND(SUM( requests  ),0) AS requests_native"                                                                                                                                                            ,"",""),
("Total Clicks"         ,"total_clicks_native"      ,"1"   ,"2"  ,"2,3" /*,1*/  ,"total_clicks_native"   ,"SUM(total_clicks_native) AS total_clicks_native"                                 ,"ROUND(SUM(total_clicks),0) AS total_clicks_native"                                                                                                                                                       ,"",""),
("Paid Clicks"          ,"paid_clicks_native"       ,"1"   ,"2"  ,"2,3" /*,1*/  ,"paid_clicks_native"    ,"SUM(paid_clicks_native) AS paid_clicks_native"                                   ,"ROUND(SUM(paid_clicks),0) AS paid_clicks_native"                                                                                                                                                         ,"",""),
("Revenue"              ,"revenue_native"           ,"1"   ,"2"  ,"2,3" /*,1*/  ,"revenue_native"        ,"SUM(revenue_native) AS revenue_native"                                           ,"ROUND(IFNULL(SUM(details.revenue * cur_api.currency_value * cur_api2.conversion_value),0),2) AS revenue_native"                                                                                            ,"",""),
("CTR"                  ,"ctr_native"               ,"1"   ,"2"  ,"2,3" /*,1*/  ,"ctr_native"            ,"ROUND((SUM(total_clicks_native)/SUM(requests_native) * 100),2) AS ctr_native"    ,NULL                                                                                                                                                                                                      ,"",""),
("RPC"                  ,"rpc_native"               ,"1"   ,"2"  ,"2,3" /*,1*/  ,"rpc_native"            ,"ROUND(SUM(revenue_native)/SUM(paid_clicks_native),2) AS rpc_native"              ,NULL                                                                                                                                                                                                      ,"",""),
("eCPM"                 ,"ecpm_native"              ,"1"   ,"2"  ,"2,3" /*,1*/  ,"ecpm_native"           ,"ROUND(SUM(revenue_native)/SUM(requests_native),2) AS ecpm_native"                ,NULL                                                                                                                                                                                                      ,"","");

INSERT INTO od_qt_metric_master
(metric_name,metric_code,product_ids,channel_ids,source_type_ids/*,partner_flag*/         ,metric_alias              ,overall_metric_formula                                                                                   ,metric_formula,metric_comment,applicable_dimension_ids) VALUES
("DFP Impression"         ,"dfp_impression_video"               ,1  ,3  ,"2"      /*,0*/               ,"dfp_impressions_video"            ,"IFNULL(ROUND(SUM(dfp_impressions_video),0),0) AS dfp_impressions_video"                                        ,"SUM(dfp_impressions) AS dfp_impressions_video"                                                ,"",""),
("Ad Request"             ,"ad_request_video"                   ,1  ,3  ,"2,3"    /*,1*/               ,"requests_video"                   ,"IFNULL(ROUND(SUM(requests_video),0),0) AS requests_video"                                                      ,"SUM(requests) AS requests_video"                                                              ,"",""),
("Ad Impressions"         ,"ad_impressions_video"               ,1  ,3  ,"2,3"    /*,1*/               ,"ad_impressions_video"             ,"IFNULL(ROUND(SUM( ad_impressions_video ),0),0) AS ad_impressions_video"                                        ,"SUM( ad_impressions ) AS ad_impressions_video"                                                ,"",""),
("Revenue"                ,"revenue_video"                      ,1  ,3  ,"2,3"    /*,1*/               ,"revenue_video"                    ,"IFNULL(ROUND(SUM(revenue_video),2),0) AS revenue_video"                                                        ,"SUM(details.revenue * cur_api.currency_value * cur_api2.conversion_value ) AS revenue_video"  ,"",""),
("Clicks"                 ,"clicks_video"                       ,1  ,3  ,"2,3"    /*,1*/               ,"clicks_video"                     ,"IFNULL(ROUND(SUM( clicks_video ),0),0) AS clicks_video"                                                        ,"SUM( clicks ) AS clicks_video"                                                                ,"",""),
("eCPM on DFP"            ,"ecpm_on_dfp_video"                  ,1  ,3  ,"2"      /*,0*/               ,"ecpm_on_dfp_video"                ,"IFNULL(ROUND(SUM(revenue_video) * 1000 / SUM(dfp_impressions_video),2),0) AS ecpm_on_dfp_video"                ,NULL,                                                                                          ,"",""),
("eCPM on Ad-Partner"     ,"ecpm_on_ad_partner_video"           ,1  ,3  ,"2,3"    /*,1*/               ,"ecpm_on_ad_partner_video"         ,"IFNULL(ROUND(SUM(revenue_video) * 1000 / SUM(requests_video),2),0) AS ecpm_on_ad_partner_video"                ,NULL,                                                                                          ,"",""),
("Fillrate on DFP"        ,"fillrate_on_dfp_video"              ,1  ,3  ,"2"      /*,0*/               ,"fillrate_on_dfp_video"            ,"IFNULL(ROUND(SUM(ad_impressions_video) / SUM(dfp_impressions_video),2),0) AS fillrate_on_dfp_video"            ,NULL,                                                                                          ,"",""),
("Fillrate on Ad-Partner" ,"fillrate_on_ad_partner_video"       ,1  ,3  ,"2,3"    /*,1*/               ,"fillrate_on_ad_partner_video"     ,"IFNULL(ROUND(SUM(ad_impressions_video) / SUM(ad_impressions_video),2),0) AS fillrate_on_ad_partner_video"      ,NULL,                                                                                          ,"","");


INSERT INTO od_qt_metric_master 
(metric_name,metric_code,product_ids,channel_ids,source_type_ids/*,partner_flag*/,metric_alias                     ,overall_metric_formula                                                                                                                                                                                                                         ,metric_formula,metric_comment,applicable_dimension_ids) VALUES 
("DFP Impression"       ,"dfp_impression_app"         ,"1"  ,"6"   ,"2"   /*,0*/  ,"5th_level_in_request_app"   ,"IF(GROUP_CONCAT(DISTINCT 5th_level_in_request_app) = 'N/A','N/A',SUM(5th_level_in_request_app)) AS 5th_level_in_request_app"                                                                                                      ,"IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',ROUND(SUM(5th_level_in_request),0)) AS 5th_level_in_request_app"                                                                                                               ,"",""),
("Ad Request"           ,"ad_request_app"             ,"1"  ,"6"   ,"2,3" /*,0*/  ,"ad_request_app"             ,"IF(GROUP_CONCAT(DISTINCT ad_request_app) = 'N/A','N/A',SUM(ad_request_app)) AS ad_request_app"                                                                                                                                    ,"IF(GROUP_CONCAT(DISTINCT IF(adunit.ad_unit_id < 0,1,0) ORDER BY 1) = '1','N/A',ROUND(SUM(in_request),0)) AS ad_request_app"                                                                                                                                   ,"",""),
("Ad Partner Impression","ad_partner_impressions_app" ,"1"  ,"6"   ,"2,3" /*,1*/  ,"ad_partner_impressions_app" ,"SUM(ad_partner_impressions_app) AS ad_partner_impressions_app"                                                                                                                                                                        ,"ROUND(SUM( ad_request  ),0) AS ad_partner_impressions_app"                                                                                                                                                                                                    ,"",""),
("Paid Impression"      ,"paid_impressions_app"       ,"1"  ,"6"   ,"2,3" /*,1*/  ,"paid_impressions_app"       ,"SUM(paid_impressions_app) AS paid_impressions_app"                                                                                                                                                                                    ,"ROUND(SUM(ad_monetize),0) AS paid_impressions_app"                                                                                                                                                                                                            ,"",""),
("Unfilled Impression"  ,"unfilled_impressions_app"   ,"1"  ,"6"   ,"2"   /*,0*/  ,"unfilled_impressions_app"   ,"SUM(unfilled_impressions_app) AS unfilled_impressions_app"                                                                                                                                                                            ,"IFNULL(ROUND(SUM(unfilled_impressions),0),0) AS unfilled_impressions_app"                                                                                                                                                                                     ,"",""),
("Fill Rate"            ,"fillrate_app"               ,"1"  ,"6"   ,"2,3" /*,1*/  ,"fillrate_app"               ,"IF(@var_partner_flag = 1,ROUND(IFNULL(SUM(paid_impressions_app)/SUM(ad_partner_impressions_app) * 100,'N/A'),2),ROUND(IFNULL(SUM(paid_impressions_app)/SUM(5th_level_in_request_app) * 100,'N/A'),2)) AS fillrate_app"    ,NULL                                                                                                                                                                                                                                                               ,"",""),
("CPM"                  ,"cpm_app"                    ,"1"  ,"6"   ,"2,3" /*,1*/  ,"cpm_app"                    ,"ROUND(IFNULL(SUM(revenue_app) * 1000 / SUM(paid_impressions_app),'N/A'),2) AS cpm_app"                                                                                                                                            ,NULL                                                                                                                                                                                                                                                               ,"",""),
("Revenue"              ,"revenue_app"                ,"1"  ,"6"   ,"2,3" /*,1*/  ,"revenue_app"                ,"SUM(revenue_app) AS revenue_app"                                                                                                                                                                                                      ,"ROUND(IFNULL(IF(provider_category_type_name = 'Direct' AND direct.account_id IS NOT NULL,SUM(ad_request * (direct_cur.currency_value*IFNULL(direct.ecpm,0) ) /1000),  SUM(revenue * cur_api.currency_value * cur_api2.conversion_value)),0),2) AS revenue_app"  ,"",""),
("eCPM"                 ,"ecpm_app"                   ,"1"  ,"6"   ,"2"   /*,0*/  ,"ecpm_app"                   ,"IF(@var_partner_flag = 1,ROUND(IFNULL(SUM(revenue_app) * 1000 /  SUM(ad_partner_impressions_app),'N/A'),2),ROUND(IFNULL(SUM(revenue_app) * 1000 /  SUM(5th_level_in_request_app),'N/A'),2)) AS ecpm_app"                  ,NULL                                                                                                                                                                                                                                                               ,"","");

INSERT INTO od_qt_metric_master 
(metric_name,metric_code,product_ids,channel_ids,source_type_ids          /*,partner_flag*/  ,metric_alias                    ,overall_metric_formula                                                                                                          ,metric_formula,metric_comment,applicable_dimension_ids) VALUES 
("Views"                ,"views_youtube"                ,"1"  ,"5"   ,"2" /*,0*/             ,"views_youtube"                 ,"IFNULL(ROUND(SUM(views_youtube),0),0) as views_youtube"                                                                        ,"IFNULL(ROUND(SUM(views),0),0) as views_youtube"                                                                                    ,"",""),
("Ad Impressions"       ,"ad_impressions_youtube"       ,"1"  ,"5"   ,"2" /*,0*/             ,"ad_impressions_youtube"        ,"IFNULL(ROUND(SUM( ad_impressions_youtube ),0),0) as ad_impressions_youtube"                                                    ,"IFNULL(ROUND(SUM( ad_impressions ),0),0) as ad_impressions_youtube"                                                                ,"",""),
("CPM"                  ,"cpm_youtube"                  ,"1"  ,"5"   ,"2" /*,0*/             ,"cpm_youtube"                   ,"IFNULL(ROUND(ROUND(SUM(youtube_ad_revenue_youtube),0) * 1000 / ROUND(SUM( ad_impressions_youtube ),0),2),0) AS cpm_youtube"    ,NULL                                                                                                                                ,"",""),
("Estimated Ad Revenue" ,"estimated_ad_revenue_youtube" ,"1"  ,"5"   ,"2" /*,0*/             ,"estimated_ad_revenue_youtube"  ,"IFNULL(ROUND(SUM(estimated_ad_revenue_youtube),2),0) AS estimated_ad_revenue_youtube"                                          ,"IFNULL(ROUND(SUM(estimated_ad_revenue * cur_api.currency_value * cur_api2.conversion_value),0),0) as estimated_ad_revenue_youtube" ,"",""),
("Youtube Ad Revenue"   ,"youtube_ad_revenue_youtube"   ,"1"  ,"5"   ,"2" /*,0*/             ,"youtube_ad_revenue_youtube"    ,"IFNULL(ROUND(SUM(youtube_ad_revenue_youtube),2),0) AS youtube_ad_revenue_youtube"                                              ,"IFNULL(ROUND(SUM(youtube_ad_revenue * cur_api.currency_value * cur_api2.conversion_value ),0),0) as youtube_ad_revenue_youtube"    ,"","");

INSERT INTO od_qt_metric_master 
(metric_name,metric_code,product_ids,channel_ids,source_type_ids          /*,partner_flag*/  ,metric_alias                    ,overall_metric_formula                                                                      ,metric_formula,metric_comment,applicable_dimension_ids) VALUES 
("Bid request"            ,"bid_request_hb"       ,"2"  ,""   ,""         /*,0*/             ,"bid_request_hb"                ,"IFNULL(ROUND(SUM(bid_request_hb),0),0) as bid_request_hb"                                  ,"IFNULL(ROUND(SUM(ph_init_request),0),0) as bid_request_hb"                                                   ,"",""),
("Bid Response"           ,"bid_response_hb"      ,"2"  ,""   ,""         /*,0*/             ,"bid_response_hb"               ,"IFNULL(ROUND(SUM(bid_response_hb),0),0) as bid_response_hb"                                ,"IFNULL(ROUND(SUM( ph_bid_before_timeout ),0),0) as bid_response_hb"                                          ,"",""),
("Wins"                   ,"wins_hb"              ,"2"  ,""   ,""         /*,0*/             ,"wins_hb"                       ,"IFNULL(ROUND(ROUND(SUM(wins_hb),0) * 1000 ,2),0) AS wins_youtube"                          ,"IFNULL(ROUND(SUM( console_wins ),0),0) as wins_hb"                                                           ,"",""),
("Revenue"                ,"revenue_hb"           ,"2"  ,""   ,""         /*,0*/             ,"revenue_hb"                    ,"IFNULL(ROUND(SUM(revenue_hb),2),0) AS revenue_hb"                                          ,"IFNULL(ROUND(SUM(console_revenue * cur_api.currency_value * cur_api2.conversion_value),0),0) as revenue_hb"  ,"",""),               
("CPM"                    ,"cpm_hb"               ,"2"  ,""   ,""         /*,0*/             ,"cpm_hb"                        ,"IFNULL(ROUND(ROUND(SUM(revenue_hb),0) * 1000 / ROUND(SUM(wins_hb),0),2),0) AS cpm_hb"      ,NULL                                                                                                          ,"",""),
("Average Response Time"  ,"art_hb"               ,"2"  ,""   ,""         /*,0*/             ,"art_hb"                        ,"IFNULL(ROUND(ROUND(SUM(art_hb),0),2),0) AS art_hb"                                         ,"IFNULL(ROUND(SUM(ph_avg_bid_response_before_timeout),2),0) AS avg_response_time"                             ,"",""),
("Timeout"                ,"timeout_hb"           ,"2"  ,""   ,""         /*,0*/             ,"timeout_hb"                    ,"IFNULL(ROUND(ROUND(SUM(timeout_hb),0),2),0) AS timeout_hb"                                 ,"IFNULL(ROUND(SUM(ph_total_timeout),0),0) AS timeout_hb"                                                      ,"","");

DROP PROCEDURE IF EXISTS sp_od_qt_list_source_type_master;
CREATE PROCEDURE sp_od_qt_list_source_type_master()
BEGIN

    SELECT source_type_id,source_type_name
    FROM od_qt_source_type_master
    WHERE active_status = 1;
END;


DROP PROCEDURE IF EXISTS sp_od_qt_list_dimensions;
CREATE PROCEDURE sp_od_qt_list_dimensions(arg_product_ids VARCHAR(255),arg_channel_ids VARCHAR(255),arg_source_type_ids VARCHAR(255))
BEGIN

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1  @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
        -- SELECT @p1, @p2;
        SELECT @p1 AS error_code, @p2 as error_msg;
    END;
    
    SET @sql_list_dimension = "";
    
    SET @sql_list_dimension = CONCAT("SELECT dimension_id,dimension_name,dimension_code,dimension_select_name
                                      FROM od_qt_dimension_master
                                      WHERE active_status = 1
                                      AND (( ",IF(arg_product_ids != "",CONCAT(" FIND_IN_SET('",REPLACE(arg_product_ids,",","',product_ids) > 0 /*OR*/ AND FIND_IN_SET('"),"',product_ids) > 0"),"1=1"),")
                                      
                                      /*",IF(arg_product_ids IN ('1','2','3','1,2,3',''),"AND","OR"),"*/ AND ( ",IF(arg_channel_ids != "",CONCAT(" FIND_IN_SET('",REPLACE(arg_channel_ids,",","',channel_ids) > 0 AND FIND_IN_SET('"),"',channel_ids) > 0"),"1=1"),"))
                                      
                                      AND ( ",IF(arg_source_type_ids != "",CONCAT(" FIND_IN_SET('",REPLACE(arg_source_type_ids,",","',source_type_ids) > 0 AND FIND_IN_SET('"),"',source_type_ids) > 0"),"1=1"),")
                                      
                                      
                                      " 
                                      );
                                      
    PREPARE STMT FROM @sql_list_dimension;
    EXECUTE STMT;
    DEALLOCATE PREPARE STMT;
                                      
    SELECT 0 AS error_code,"Success" AS error_msg;

END;


-- CALL sp_od_qt_list_dimensions("1","1","");
-- CALL sp_od_qt_list_dimensions("2","","");
-- CALL sp_od_qt_list_dimensions("3","","");
-- CALL sp_od_qt_list_dimensions("1,2","","");
-- CALL sp_od_qt_list_dimensions("1,2","1,2","");

DROP PROCEDURE IF EXISTS sp_od_qt_list_metric;
CREATE PROCEDURE sp_od_qt_list_metric(arg_product_ids VARCHAR(255),arg_channel_ids VARCHAR(255),arg_source_type_ids VARCHAR(255))
BEGIN

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1  @p1 = RETURNED_SQLSTATE, @p2 = MESSAGE_TEXT;
        -- SELECT @p1, @p2;
        SELECT @p1 AS error_code, @p2 as error_msg;
    END;
    
    SET @sql_list_metric = "";
    
    SET @sql_list_metric =    CONCAT("SELECT metric_id,metric_name,metric_code,
                                              product.product_id,product.product_name,
                                              channel.channel_id,channel.channel,
                                              IF(channel.channel IS NOT NULL,channel.channel,product.product_name) AS metric_category,
                                              IF(
                                              (
                                                  ( ",IF(arg_product_ids != "",CONCAT(" FIND_IN_SET('",REPLACE(arg_product_ids,",","',product_ids) > 0 OR FIND_IN_SET('"),"',product_ids) > 0"),"1=1"),")
                                              AND 
                                                  ( ",IF(arg_channel_ids != "",CONCAT(" FIND_IN_SET('",REPLACE(arg_channel_ids,",","',channel_ids) > 0 OR FIND_IN_SET('"),"',channel_ids) > 0"),"1=1"),")
                                              AND 
                                                  ( ",IF(arg_source_type_ids != "",CONCAT(" FIND_IN_SET('",REPLACE(arg_source_type_ids,",","',source_type_ids) > 0 OR FIND_IN_SET('"),"',source_type_ids) > 0"),"1=1"),")
                                              ),1,0) AS enable_flag
                                      FROM od_qt_metric_master metric
                                      LEFT JOIN od_product_master product ON metric.product_ids = product.product_id AND product.active_status = 1
                                      LEFT JOIN od_channel_master channel ON metric.channel_ids = channel.channel_id AND channel.active_status = 1
--                                       WHERE ( ",IF(arg_product_ids != "",CONCAT(" FIND_IN_SET('",REPLACE(arg_product_ids,",","',product_ids) > 0 AND FIND_IN_SET('"),"',product_ids) > 0"),"1=1"),")
--                                       AND ( ",IF(arg_channel_ids != "",CONCAT(" FIND_IN_SET('",REPLACE(arg_channel_ids,",","',channel_ids) > 0 AND FIND_IN_SET('"),"',channel_ids) > 0"),"1=1"),")
--                                       AND ( ",IF(arg_source_type_ids != "",CONCAT(" FIND_IN_SET('",REPLACE(arg_source_type_ids,",","',source_type_ids) > 0 AND FIND_IN_SET('"),"',source_type_ids) > 0"),"1=1"),")
                                      " 
                                      );
                                      
    PREPARE STMT FROM @sql_list_metric;
    EXECUTE STMT;
    DEALLOCATE PREPARE STMT;
                                      
    SELECT 0 AS error_code,"Success" AS error_msg;

END;

-- CALL sp_od_qt_list_metric("","","");
-- CALL sp_od_qt_list_metric("1","1","");
-- CALL sp_od_qt_list_metric("2","","");
-- CALL sp_od_qt_list_metric("3","","");
-- CALL sp_od_qt_list_metric("1,2","","");
-- CALL sp_od_qt_list_metric("1,2","1,2","");

DROP FUNCTION IF EXISTS fn_od_select_partition_names_v3;
CREATE FUNCTION fn_od_select_partition_names_v3(
    arg_table_name VARCHAR(255),
    arg_account_id VARCHAR(255),
    arg_start_day_id DATE,
    arg_end_day_id DATE
) RETURNS mediumtext CHARSET latin1
    DETERMINISTIC
BEGIN
    
    DECLARE i INT DEFAULT 1;
    
    SET @var_partion_name_list ="";
    SET @var_string_length = CHAR_LENGTH(arg_account_id) - CHAR_LENGTH( REPLACE ( arg_account_id ,',', ''))  + 1; 

    WHILE (i <= @var_string_length )  DO 
    
    SET @var_account_id = 0;
    SET @var_account_id = SUBSTRING_INDEX (SUBSTRING_INDEX( arg_account_id,',',i),',', -1  );
    
    SET @var_partion_name = "";
    
    SET @var_partion_name = (SELECT GROUP_CONCAT( DISTINCT b.PARTITION_NAME) AS partition_names
            FROM (
            
                    SELECT CONCAT("p",@var_account_id,YEAR((arg_start_day_id+INTERVAL (H+T+U) day)),MONTH((arg_start_day_id+INTERVAL (H+T+U) day)))  as day_par_id
                    FROM ( SELECT 0 H
                      UNION ALL SELECT 100 UNION ALL SELECT 200 UNION ALL SELECT 300
                    ) H CROSS JOIN ( SELECT 0 T
                      UNION ALL SELECT  10 UNION ALL SELECT  20 UNION ALL SELECT  30
                      UNION ALL SELECT  40 UNION ALL SELECT  50 UNION ALL SELECT  60
                      UNION ALL SELECT  70 UNION ALL SELECT  80 UNION ALL SELECT  90
                    ) T CROSS JOIN ( SELECT 0 U
                      UNION ALL SELECT   1 UNION ALL SELECT   2 UNION ALL SELECT   3
                      UNION ALL SELECT   4 UNION ALL SELECT   5 UNION ALL SELECT   6
                      UNION ALL SELECT   7 UNION ALL SELECT   8 UNION ALL SELECT   9
                    ) U   
                    WHERE (arg_start_day_id+INTERVAL (H+T+U) day) BETWEEN arg_start_day_id and arg_end_day_id
                    GROUP by day_par_id
            ) a
            JOIN INFORMATION_SCHEMA.PARTITIONS b 
                          ON b.TABLE_SCHEMA = DATABASE()
                          AND b.TABLE_NAME = arg_table_name
--                        AND b.PARTITION_NAME != ''
--                        AND b.PARTITION_NAME IS NOT NULL
                          AND b.PARTITION_NAME = a.day_par_id
    );
    

    SET @var_partion_name_list = CONCAT(@var_partion_name_list,IF(IFNULL(@var_partion_name,"") = "","",CONCAT(",",IFNULL(@var_partion_name,"")))  );
    
    SET i=i+1 ;
    END WHILE;
    
    RETURN IF(IFNULL(@var_partion_name_list,"") = "","",CONCAT("PARTITION (",TRIM(BOTH ',' FROM @var_partion_name_list),")"));
END;

DROP FUNCTION IF EXISTS fn_od_select_partition_names_v4;
CREATE FUNCTION fn_od_select_partition_names_v4(
    arg_table_name VARCHAR(255),
    arg_start_day_id DATE,
    arg_end_day_id DATE
) RETURNS mediumtext CHARSET latin1
    DETERMINISTIC
BEGIN
    
    SET @var_partion_name = "";
    
    SET @var_partion_name = (SELECT GROUP_CONCAT( DISTINCT b.PARTITION_NAME) AS partition_names
            FROM (
            
                    SELECT CONCAT(YEAR((arg_start_day_id+INTERVAL (H+T+U) day)),MONTH((arg_start_day_id+INTERVAL (H+T+U) day))) AS year_month_id,
                           LENGTH(CONCAT(YEAR((arg_start_day_id+INTERVAL (H+T+U) day)),MONTH((arg_start_day_id+INTERVAL (H+T+U) day)))) AS length_par
                    FROM ( SELECT 0 H
                      UNION ALL SELECT 100 UNION ALL SELECT 200 UNION ALL SELECT 300
                    ) H CROSS JOIN ( SELECT 0 T
                      UNION ALL SELECT  10 UNION ALL SELECT  20 UNION ALL SELECT  30
                      UNION ALL SELECT  40 UNION ALL SELECT  50 UNION ALL SELECT  60
                      UNION ALL SELECT  70 UNION ALL SELECT  80 UNION ALL SELECT  90
                    ) T CROSS JOIN ( SELECT 0 U
                      UNION ALL SELECT   1 UNION ALL SELECT   2 UNION ALL SELECT   3
                      UNION ALL SELECT   4 UNION ALL SELECT   5 UNION ALL SELECT   6
                      UNION ALL SELECT   7 UNION ALL SELECT   8 UNION ALL SELECT   9
                    ) U   
                    WHERE (arg_start_day_id+INTERVAL (H+T+U) day) BETWEEN arg_start_day_id and arg_end_day_id
                    GROUP by year_month_id
            ) a
            JOIN INFORMATION_SCHEMA.PARTITIONS b 
                          ON b.TABLE_SCHEMA = DATABASE()
                          AND b.TABLE_NAME = arg_table_name
                          AND RIGHT(b.PARTITION_NAME,length_par) = a.year_month_id
    );

    RETURN IF(IFNULL(@var_partion_name,"") = "","",CONCAT("PARTITION (",TRIM(BOTH ',' FROM @var_partion_name),")"));
END;


