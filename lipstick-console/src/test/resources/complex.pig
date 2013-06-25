REGISTER 's3n://netflix-dataoven-prod/genie/jars/dse_pig.jar';
REGISTER 's3n://netflix-dataoven-prod-users/DSE/etl_code/common/python/nflx_common_eval.py' using jython as eval;

set default_parallel 50;
set dse.partitionedstorer.hiveDelim true;

--Following statements to get the old data from device_model_d
base_dev_mod0 = load 'prodhive.etl.device_model_d' using DseStorage();

base_dev_mod1 = foreach base_dev_mod0 generate 
                device_type_id as device_type_id:int,
                (device_model_header == '\\N' ? null : device_model_header) as device_model_header:chararray,
                (device_form_factor == '\\N' ? null : device_form_factor) as device_form_factor:chararray,
                device_type_desc as dev_type_desc:chararray,
                device_type_name as dev_type_name:chararray,
                brand as brand:chararray,
                manufacturer as manufacturer:chararray,
                is_googletv as is_googletv:int,
                insert_date as insert_date:int;

base_dev_mod = foreach base_dev_mod1 generate 
               device_type_id as device_type_id:int,(
               device_model_header is null ? 'na' : device_model_header) as device_model_header:chararray,
               (device_form_factor is null ? 'na' : device_form_factor) as device_form_factor:chararray,
               dev_type_desc as dev_type_desc:chararray,
               dev_type_name as dev_type_name:chararray,
               brand as brand:chararray,manufacturer as manufacturer:chararray,
               is_googletv as is_googletv:int,
               insert_date as insert_date:int;

-- Following statements are to read new device__tyep_id,device_model_header and device_form_factor from device_esn_d
base_dev0 = load 'prodhive.dse.device_esn_d' using DseStorage();

base_dev1 = foreach base_dev0 generate 
            esn as esn:chararray,
            device_type_id as devtype_id:int,
            (device_model == '\\N' ? null : device_model) as devmod:chararray,
            (device_form_factor == '\\N' ? null :device_form_factor) as devicecategory:chararray;

base_dev = foreach base_dev1 generate 
           esn as esn:chararray,
           devtype_id as devtype_id:int,
           (devmod is null ? 'na' : devmod) as devmod:chararray,
           (devicecategory is null ? 'na' : devicecategory) as devicecategory:chararray;

--Following statment is read all device type ids from nrd portal source
dev_mod = load 'prodhive.etl.eds_device_types' using DseStorage(); 

devmod1 = foreach base_dev generate devtype_id as devtype_id:int,devmod as devmod:chararray,devicecategory as device_form_factor:chararray;

devmod2 = DISTINCT devmod1;

SPLIT devmod2 into and_devmod IF devtype_id == 419 , non_and_devmod IF devtype_id != 419;-- Spilit the device_esn_d data to 419 and non 419

devmod4 = foreach and_devmod generate 
          devtype_id as devtype_id:int,
          devmod as devmod:chararray,
          device_form_factor as device_form_factor:chararray,
          devmod as dev_type_desc:chararray,
          devmod as dev_type_name:chararray,
          TRIM(REPLACE(SUBSTRING(devmod,0,5),'=','')) as Brand:chararray,
          TRIM(REPLACE(SUBSTRING(devmod,0,5),'=','')) as manufacturer:chararray,
          0 as is_googletv:int; -- For 419 device_type_id device_esn_d is the source

devmod5 = JOIN dev_mod by device_type_id LEFT,non_and_devmod BY devtype_id parallel 256;

devmod6 = foreach devmod5 generate 
          dev_mod::device_type_id as devtype_id:int,
          non_and_devmod::devmod as devmod:chararray,
          non_and_devmod::device_form_factor as device_form_factor,
          dev_mod::device_type_desc as dev_type_desc:chararray,
          dev_mod::device_type_name as dev_type_name:chararray,
          dev_mod::brand as brand:chararray,
          dev_mod::manufacturer as manufacturer:chararray,
          dev_mod::googletv_indicator as is_googletv:int; --For non 419 device type ids ndrp portal database is source

devmod7 = UNION devmod6,devmod4;-- joining 419 and non 419 device type as one source

devmod8 = foreach devmod7 generate 
          devtype_id as device_type_id:int,
          (devmod is null ? 'na' : devmod) as device_model_header:chararray,
          (device_form_factor is null ? 'na' : device_form_factor) as device_form_factor:chararray,
          dev_type_desc as dev_type_desc:chararray,
          dev_type_name as dev_type_name:chararray,
          brand as brand:chararray,
          manufacturer as manufacturer:chararray,
          is_googletv as is_googletv:int,
          1 as insert_date:int;

devmod9 = JOIN devmod8 by (device_type_id,device_model_header,device_form_factor) LEFT,base_dev_mod by (device_type_id,device_model_header,device_form_factor) parallel 256; -- Join to get new combination of device_type_id,device_model_header and device_form_factor

devmod10 = FILTER devmod9 by (base_dev_mod::device_type_id is null) and (base_dev_mod::device_model_header is null) and (base_dev_mod::device_form_factor is null);-- Filter to get new records

devmod11 = foreach devmod10 generate
           devmod8::device_type_id as device_type_id:int,
           devmod8::device_model_header as device_model_header:chararray,
           devmod8::device_form_factor as  device_form_factor:chararray,
           devmod8::dev_type_desc as dev_type_desc:chararray,
           devmod8::dev_type_name as dev_type_name:chararray,
           devmod8::brand as brand:chararray,
           devmod8::manufacturer as manufacturer:chararray,
           devmod8::is_googletv as is_googletv:int,
           devmod8::insert_date as insert_date:int;

devmod12 = UNION base_dev_mod,devmod11;-- Union the existing data set with ewn data set to get a new dataset

devmod13 = foreach devmod12 generate 
           device_type_id as device_type_id:int,
           (device_model_header is null ? '\\N' : device_model_header) as device_model_headel:chararray,
           (device_form_factor is null ? '\\N' : device_form_factor) as device_form_factor:chararray,
           dev_type_desc as dev_type_desc:chararray,
           dev_type_name as dev_type_name:chararray,
           brand as brand:chararray,
           manufacturer as manufacturer:chararray,
           is_googletv as is_googletv:int,
           insert_date as insert_date:int,
           (long)1 as batchid;

devmod14 = LIMIT devmod13 100;

store devmod14 into 's3n://netflix-dataoven-prod-users/hive/warehouse/etl.db/device_model_d' using com.netflix.dse.PartitionedStorer('gz','batchid');
