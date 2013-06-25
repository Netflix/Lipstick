register 's3n://netflix-dataoven-prod/genie/jars/dse_pig-2.0.0.jar';
register 's3n://netflix-dataoven-prod-users/DSE/etl_code/common/python/nflx_common_datetime.py' using jython as datetime;
--REGISTER 's3n://netflix-dataoven-prod/genie/jars/dse_pig.jar';

set dse.partitionedstorer.hiveDelim true;
SET default_parallel 100;
DEFINE LENGTH org.apache.pig.piggybank.evaluation.string.LENGTH();
DEFINE DataovenLoader com.netflix.hadoop.hcatalog.DataovenLoader();

country_region = load 'ragrawal.region_map' using DataovenLoader();
region_num_strms_threshold = load 'ragrawal.region_threshold_num_strms' using DataovenLoader();
searchevents = load 'default.searchevents' using DataovenLoader();
streaming_session = load 'default.dse_streaming_session_info' using DataovenLoader();
ttl_title_country_r = load 'dse.ttl_title_country_r' using DataovenLoader();
ttl_show_d = load 'dse.ttl_show_d' using DataovenLoader();

srch_ts = filter searchevents by dateint >= 20121023 and dateint <= 20121023;

srch_cols = foreach srch_ts generate 
                other_properties#'eccust_id' as eccust_id,
                LOWER(TRIM(other_properties#'exec_query')) as query,
                dateint as dateint,
                (event_utc_ms/1000.0) as srch_ts:double,
                other_properties#'country' as country;

flt_srch = filter srch_cols by 
                    query is not null
                    and LENGTH(query) > 0;
                
join_srch_region = join flt_srch by country, country_region by country_iso_code using 'replicated';

grp_srch = group join_srch_region by (eccust_id, query, dateint, country, region);


srch = foreach grp_srch generate 
                (long)group.eccust_id as eccust_id:long,
                group.query as query,
                group.dateint as dateint,
                MIN(join_srch_region.srch_ts) as srch_ts,
                group.country as country,
                group.region as region;

-- 1.2 get plays
flt_strm = filter streaming_session by
                stop_received_time_ms>start_received_time_ms
                and dateint >= 20120101 and dateint <= 20120131;
                 
                                       
strm = foreach flt_strm {
        playback_start_time_sec = (double) start_received_time_ms/1000.0;
        playback_stop_time_sec = (double) stop_received_time_ms/1000.0;
        mins = (double)(playback_stop_time_sec - playback_start_time_sec)/60.0;
        generate
            customer_id,
            dateint,
            title_id,
            playback_start_time_sec as playback_start_time_sec:double,
            playback_stop_time_sec as playback_stop_time_sec:double,
            play_iso_country_code,
            mins as mins;    
        };


-- 1.3 Get active titles
flt_ttl_ctry = filter ttl_title_country_r by
                    last_display_date >= 20120131 and first_display_date <= 20120131;
            
join_show_ttl_ctry = join ttl_show_d by show_title_id, flt_ttl_ctry by show_title_id;

ttl_dirty = foreach join_show_ttl_ctry generate
                ttl_show_d::show_desc as show_name,
                ttl_show_d::show_title_id as show_movie_id,
                flt_ttl_ctry::title_id as movie_id,
                flt_ttl_ctry::country_iso_code as country_code;

ttl = distinct ttl_dirty;


-- 1.4 join srch, strm, ttl   
join_srch_strm = join srch by (eccust_id, dateint, country), strm by (customer_id, dateint, play_iso_country_code) parallel 150;


-- only movies/shows played after search are retrained
flt_srch_strm = filter join_srch_strm by srch_ts <= playback_start_time_sec;


join_srch_strm_ttl = join ttl by (movie_id, country_code), flt_srch_strm by (title_id, country) parallel 150;


-- for each day account and region, extract only one search-movie pair
grp_srch_strm_ttl = group join_srch_strm_ttl by (                                                                                                   
                        eccust_id,
                        srch::dateint,
                        query,
                        srch_ts,
                        show_movie_id,
                        show_name,
                        region
                    ) parallel 150;

pas_ts_all = foreach grp_srch_strm_ttl generate 
                group.eccust_id as eccust_id, 
                group.dateint as dateint, 
                group.query as query,
                (int)group.srch_ts as srch_ts:int,
                group.show_movie_id as show_movie_id,
                group.show_name as show_name,
                (int)MIN(join_srch_strm_ttl.playback_start_time_sec) as strm_ts:int,
                (int)SUM(join_srch_strm_ttl.mins) as mins:int,
                group.region as region;


--store pas_ts_all into 'prodhive.ragrawal.pas_ts' using DseBatchedStorage('gz');
store pas_ts_all into 'temp-pas_ts_all';

-- Step 2: calculate strength between search and movie.
-- There are two different ways to calculate strength
-- 1. Based on delay: the longer the delay between search and playing a movie, the lower is the strength
--       num_strms = 60/(60 + delay_in_minutes )
-- 2. Based on combination of delay and how long a movie is played: play_length_in_minutes * num_strms

pas_all_cols = foreach pas_ts_all {
                srch_hr = (int)datetime.format_epoch('%H', srch_ts);
                srch_min = (int)datetime.format_epoch('%M', srch_ts);
                strm_hr = (int)datetime.format_epoch('%H', strm_ts);
                strm_min = (int)datetime.format_epoch('%M', strm_ts);
                delay = (strm_hr - srch_hr)*60.0 + (strm_min - srch_min);
    
                -- delay = FLOOR((strm_ts - srch_ts)/(double)60.0);
                num_strms = (double)60.0/((double)60.0 + delay);
                hrs = (double)mins*num_strms;
                generate 
                    TRIM(query) as query, 
                    show_movie_id as show_movie_id,
                    show_name as show_name,
                    num_strms as num_strms,
                    hrs as hrs,
                    region as region,
                    delay as delay;
            };

-- TOOD: ALL THIS SHOULD BE HANDLED IN THE FIRST STEP
flt_pas = filter pas_all_cols by 
                query is not null
                and LENGTH(query) > 0
                and delay >= 0 ;

grp_pas_all = group flt_pas by (region, query, show_movie_id, show_name);

pas_all_ncols = foreach grp_pas_all generate
                    group.query as query,
                    group.show_movie_id as show_movie_id,
                    group.show_name as show_name,
                    SUM(flt_pas.num_strms) as num_strms,
                    SUM(flt_pas.hrs)/60.0 as hrs,
                    group.region as region;
                    

-- only retain those pairs that passes minimum threshold criteria
join_pas_threshold = join pas_all_ncols by region LEFT OUTER, region_num_strms_threshold by region using 'replicated';

flt_pas_all = filter join_pas_threshold by pas_all_ncols::num_strms >= (region_num_strms_threshold::num_strms is null ? 5 : region_num_strms_threshold::num_strms) ;

pas_all = foreach flt_pas_all generate
                query as query,
                show_movie_id as show_movie_id,
                show_name as show_name,
                (int) (pas_all_ncols::num_strms) as num_strms:int,
                hrs as hrs:double,                
                pas_all_ncols::region as region;
               
                                    
--store pas_all into 'prodhive.ragrawal.pas_all' using DseBatchedStorage('gz');
store pas_all into 'temp-pass_all';

-- Step 3: search_plays_after_search_probs_all 
-- P(q->t) = #(q->t)/\sum(q)\sum(t)#q->t
-- P(t) = #t/\sum(#t)

-- calculate total strength for show itself i.e n(t)                
grp_ttl = group pas_all by  (region, show_movie_id);
stats_ttl = foreach grp_ttl generate
                flatten(group) as (region, show_movie_id),
                (double) SUM(pas_all.num_strms) as total_strms_show: double,
                (double) SUM(pas_all.hrs) as total_hrs_show:double;


-- calculate total strength for queries i.e. n(q) 
-- this is required for noise calculation
grp_q = group pas_all by (region, query);
stats_q = foreach grp_q generate
                flatten(group) as (region, query),
                (double) SUM(pas_all.num_strms) as total_strms_query: double,
                (double) SUM(pas_all.hrs) as total_hrs_query: double ;
  
-- aggregate at region level
grp_region = group pas_all by region;
stats_region = foreach grp_region generate
                    flatten(group) as region,
                    (double) SUM(pas_all.num_strms) as all_strms:double,
                    (double) SUM(pas_all.hrs) as all_hrs:double;
                  
-- expand pas_all by attaching query_stats and show stats
pas_q = join pas_all by (region, query), stats_q by (region, query);
pas_q_ttl = join pas_q by (pas_all::region, show_movie_id),
                      stats_ttl by (region, show_movie_id);


-- calculate streaming and query related factors
pas_probs = foreach pas_q_ttl generate
            pas_q::pas_all::query as query,
            pas_q::pas_all::show_movie_id as show_movie_id,
            show_name,
            (int)num_strms as num_strms:int,
            (
                total_strms_query > 0.0 
                ? (double)num_strms/total_strms_query
                : 0.0
            ) as p_strms,
            (
                total_hrs_query > 0.0 
                ? (double)hrs/total_hrs_query 
                : 0.0 
            ) as p_hrs,
            (
                num_strms > 1.0 and total_strms_show > 1.0 and total_strms_query > 1.0
                ? 1.0 + 1.0/total_strms_show - 1*SQRT(1.0/num_strms + 1.0/total_strms_show + 1.0/(total_strms_show*total_strms_show) - 1.0/total_strms_query)
                : 1.0
            ) as noise_strms,
            (
                hrs > 1.0 and total_hrs_show > 1.0 and total_hrs_query > 1.0
                ? 1.0 + 1.0/total_hrs_show - 1*SQRT(1.0/hrs + 1.0/total_hrs_show + 1.0/(total_hrs_show*total_hrs_show) - 1.0/total_hrs_query)
                : 1.0 
            ) as noise_hrs,
            pas_q::pas_all::region as region;
            
--store pas_probs into 'prodhive.ragrawal.pas_probs' using DseBatchedStorage('gz');
store pas_probs into 'temp-pas_probs';

          

-- 4.3.3: Join show stats and region stats 
join_stats_ttl_region = join stats_ttl by region, stats_region by region using 'replicated';
pas_pops  = foreach join_stats_ttl_region generate
                    show_movie_id as show_movie_id,
                    total_strms_show as total_strms,
                    (int) total_hrs_show as total_hrs:int,
                    (
                        all_strms > 0.0 
                        ? (double)total_strms_show/all_strms 
                        : 0.0
                    ) as pop_strms:double,
                    (
                        all_hrs > 0.0
                        ? (double)total_hrs_show/all_hrs
                        : 0.0
                    ) as pop_hrs:double,
                    stats_region::region as region;

--store pas_pops into 'prodhive.ragrawal.pas_pops' using DseBatchedStorage('gz');
store pas_pops into 'temp-pass_pops';

-- Join query, show and countrygroup stats together 
join_probs_pops = join pas_probs by (show_movie_id, region),
                       pas_pops by (show_movie_id, region);
                    
pas_or = foreach join_probs_pops {
            strms_or = (
                pop_strms > 0.0
                ? p_strms/pop_strms
                : 0.0
            );
            
            hrs_or =  (
                pop_hrs > 0.0
                ? p_hrs/pop_hrs
                : 0.0
            );
            
            generate
                query as query,
                pas_probs::show_movie_id as show_movie_id,
                show_name as show_name,
                num_strms as num_strms,
                p_strms as p_strms,
                p_hrs as p_hrs,
                noise_strms as noise_strms,
                noise_hrs as noise_hrs,
                pop_strms as pop_strms,
                pop_hrs as pop_hrs,
                strms_or as strms_or,
                hrs_or as hrs_or,
                (
                    noise_strms >= 0.0 and noise_strms <=1.0 
                    ? strms_or * noise_strms
                    : (
                            noise_strms > 1.0
                            ? strms_or
                            : 0.0
                      )
                ) as strms_or_lb,
                (
                    noise_hrs >= 0.0 and noise_hrs <= 1.0
                    ? hrs_or * noise_hrs
                    : (
                            noise_hrs > 1.0 
                            ? hrs_or
                            : 0.0
                      )
                ) as hrs_or_lb,
                pas_probs::region as region;
        };

store pas_or into 'temp-pas_or';

