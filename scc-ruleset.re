########################### START ACC-POST-PROC-FOR-PUT ##########################
acPostProcForPut {
*filepath = $objPath
msiSplitPath(*filepath,*Coll,*File);

# set default metadata if not defined
set_def_metatada(*filepath,"put");

# add file.nc metadata
if ($objPath like "*.nc") {
set_nc_metadata(*filepath);
}

# set datasetid if defined in metadata collection
   *rs = select META_COLL_ATTR_NAME, META_COLL_ATTR_VALUE where META_COLL_ATTR_NAME = 'datasetid' AND COLL_NAME = '*Coll';
  foreach(*row in *rs) {
      *Str = *row.META_COLL_ATTR_NAME ++ "=" ++ *row.META_COLL_ATTR_VALUE
      msiString2KeyValPair(*Str,*Keyval);
      if (msiSetKeyValuePairsToObj(*Keyval,*filepath,"-d") == 0) {
           writeLine("serverLog", "Set *Keyval to file *filepath success");
      } else {
           writeLine("serverLog", "Set *Keyval to file *filepath failed");
           fail;
      }
  }

# end of acPostProcForPut
}

########################### END ACC-POST-PROC-FOR-PUT AND RELATIVE FUNCTION #############################

########################### START ACC-POST-PROC-FOR-COLL-CREATE AND RELATIVE FUNCTION #########################

acPostProcForCollCreate {
  *CollPath = $collName
  msiSplitPath(*CollPath,*Parent,*Coll);
   *rs = select META_COLL_ATTR_NAME, META_COLL_ATTR_VALUE where META_COLL_ATTR_NAME = 'datasetid' AND COLL_NAME = '*Parent';
  foreach(*row in *rs) {
      *Str = *row.META_COLL_ATTR_NAME ++ "=" ++ *row.META_COLL_ATTR_VALUE
      msiString2KeyValPair(*Str,*Keyval);
      if (msiSetKeyValuePairsToObj(*Keyval,*CollPath,"-C") == 0) {
           writeLine("serverlog", "Set *Keyval to collection *CollPath success");
      } else {
           writeLine("serverlog", "Set *Keyval to collection *CollPath failed");
           fail;
      }
    writeLine("serverlog", "Set *Keyval to collection *CollPath success");
  }
}
########################### END ACC-POST-PROC-FOR-COLL-CREATE AND RELATIVE FUNCTION #########################

############################ START ACC-POST-PROC-FOR-FILE-REG AND RELATIVE FUNCTION #########################

acPostProcForFilePathReg {
*filepath = $objPath
msiSplitPath(*filepath,*Coll,*File);

# set default metadata if not defined
set_def_metatada(*filepath,"reg");

# add file.nc metadata 
if ($objPath like "*.nc") {
set_nc_metadata(*filepath); 
}

# set datasetid if defined in collection

   *rs = select META_COLL_ATTR_NAME, META_COLL_ATTR_VALUE where META_COLL_ATTR_NAME = 'datasetid' AND COLL_NAME = '*Coll';
  foreach(*row in *rs) {
      *Str = *row.META_COLL_ATTR_NAME ++ "=" ++ *row.META_COLL_ATTR_VALUE
      msiString2KeyValPair(*Str,*Keyval);
      if (msiSetKeyValuePairsToObj(*Keyval,*filepath,"-d") == 0) {
           writeLine("serverLog", "Set *Keyval to file *filepath success");
      } else {
           writeLine("serverLog", "Set *Keyval to file *filepath failed");
           fail;
      }
  }

#end accpostprocforfilereg 
}
###########################END ACC-POST-PROC-FOR-FILE-REG AND RELATIVE FUNCTION #########################

###########################START ACC-POST-PROC-FOR-OPEN AND RELATIVE FUNCTION #########################
#acPostProcForOpen {
#ON($writeFlag == "1"){
#*filepath = $objPath
#writeLine("serverLog", "************* AC POST PROC FOR OPEN***********");
#
# set default metadata if not defined
#set_def_metatada(*filepath,"open");
#
# add file.nc metadata
#  if ($objPath like "*.nc") {
#  set_nc_metadata(*filepath);
#  }
# }
#}

######################## FUNCTION #######################

# set metadata if not defined 
set_metatada(*rs_metadata,*default,*metadata,*file) {
       *add = true;
       foreach(*row in *rs_metadata) {
       *add = false;
       }
       if ( *add ){
        msiString2KeyValPair(*default,*Keyval);
        if (msiSetKeyValuePairsToObj(*Keyval,*file,"-d") == 0) {
           writeLine("serverLog", "Set default metadata *default to file *file success");}
        else {
           writeLine("serverLog", "Set default metadata *default to file *file failed");
           fail;}
       }
     }

#set default value to metadata storagetype - disasterrecovery - replica - retention - import
set_def_metatada(*file,*operation) {
msiSplitPath(*file,*Coll,*File);
 if ("*operation" == "reg") {
       *rs_storagetype = select META_DATA_ATTR_VALUE, META_DATA_ATTR_NAME where DATA_NAME = '*File' and COLL_NAME = '*Coll' and META_DATA_ATTR_NAME = 'storagetype';
set_metatada(*rs_storagetype,"storagetype=registered","storagetype",*file); }
else {
    *rs_storagetype = select META_DATA_ATTR_VALUE, META_DATA_ATTR_NAME where DATA_NAME = '*File' and COLL_NAME = '*Coll' and META_DATA_ATTR_NAME = 'storagetype';
set_metatada(*rs_storagetype,"storagetype=automatic","storagetype",*file); } 


       *rs_disasterrecovery = select META_DATA_ATTR_VALUE, META_DATA_ATTR_NAME where DATA_NAME = '*File' and COLL_NAME = '*Coll' and META_DATA_ATTR_NAME = 'disasterrecovery';
set_metatada(*rs_disasterrecovery,"disasterrecovery=no","disasterrecovery",*file);

       *rs_replica = select META_DATA_ATTR_VALUE, META_DATA_ATTR_NAME where DATA_NAME = '*File' and COLL_NAME = '*Coll' and META_DATA_ATTR_NAME = 'replica';
set_metatada(*rs_replica,"replica=no","replica",*file);

       *rs_retention = select META_DATA_ATTR_VALUE, META_DATA_ATTR_NAME where DATA_NAME = '*File' and COLL_NAME = '*Coll' and META_DATA_ATTR_NAME = 'retention';
set_metatada(*rs_retention,"retention=3y","retention",*file);

       *rs_import = select META_DATA_ATTR_VALUE, META_DATA_ATTR_NAME where DATA_NAME = '*File' and COLL_NAME = '*Coll' and META_DATA_ATTR_NAME = 'import';
set_metatada(*rs_import,"import=yes","import",*file);
}

# set default valute to metadata project_id
set_def_projectid_metatada(*file) {
msiSplitPath(*file,*Coll,*File);
       *rs_project_id = select META_DATA_ATTR_VALUE, META_DATA_ATTR_NAME where DATA_NAME = '*File' and COLL_NAME = '*Coll' and META_DATA_ATTR_NAME = 'project_id';
set_metatada(*rs_project_id,"project_id=CMCC","project_id",*file);
}


set_nc_metadata(*file) {
# addMetaNetcdf - (for file registration)
# Extract  attributes from netcdf and add as AVUs to the data object
#
*filepath = $objPath
msiSplitPath(*filepath,*Coll,*File);

       *ncFilePath = $objPath
       if (msiNcOpen (*ncFilePath, "0", *ncid) == 0) {
           writeLine("serverLog", "msiNcOpen success *ncid");
       } else {
           writeLine("serverLog", "msiNcOpen failed");
           fail;
       }

#verify import metadata
    *rs_import = select META_DATA_ATTR_VALUE where META_DATA_ATTR_NAME = 'import' AND COLL_NAME = '*Coll' and DATA_NAME = '*File'
  foreach(*row in *rs_import) {
      *Str_import = *row.META_DATA_ATTR_VALUE
      if ( "*Str_import" == "yes" ) {
       if (msiNcRegGlobalAttr(*ncFilePath, "0", *status) == 0) {
           set_def_projectid_metatada(*filepath);
           writeLine("serverLog", "msiNcRegGlobalAttr success *status");}
         else {
           writeLine("serverLog", "msiNcRegGlobalAttr failed");
          fail;}
       }
     }

# do the big inquery
       if (msiNcInq (*ncid, *ncInqOut) == 0) {
          writeLine("serverLog", "msiNcInq success");
       } else {
          writeLine("serverLog", "msiNcInq failed");
          fail;
       }
# get the number of variables - *ngvars from the inquery output
       if (msiNcGetNvarsInInqOut (*ncInqOut, *ngvars) == 0) {
           writeLine("serverLog", "Global number of vars = *ngvars");
       } else {
           writeLine("serverLog", "msiNcGetNvarsInInqOut failed");
           fail;
       }
# loop over the *ngvars for setting attributes as AVUs 
       for(*I=0;*I<*ngvars;*I=*I+1) {
         if (msiNcGetVarNameInInqOut (*ncInqOut, *I, *varName) != 0) {
            writeLine("serverLog", "msiNcGetVarNameInInqOut failed");
            fail;
         }

         if (msiNcGetVarTypeInInqOut (*ncInqOut, *varName, *dataType) != 0) {
            writeLine("serverLog", "msiNcGetVarTypeInInqOut failed");
            fail;
         }

         if (msiNcIntDataTypeToStr (*dataType, *dataTypeStr) != 0) {
            writeLine("serverLog", "msiNcIntDataTypeToStr failed");
            fail;
         }
# get attribute unit from var 
         if (msiNcGetNattsInInqOut (*ncInqOut, *varName, *natts) == 0) {
          for(*J=0;*J<*natts;*J=*J+1) {
            if (msiNcGetAttNameInInqOut (*ncInqOut, *J, *varName, *attName) != 0) {
                writeLine("serverLog", "msiNcGetAttNameInInqOut failed");
                fail;
            }
            if (msiNcGetAttValStrInInqOut (*ncInqOut, *J, *varName, *attValStr) != 0) {
                writeLine("serverLog", "msiNcGetAttValStrInInqOut failed");
                fail;
            }
	    msiSubstr(*attValStr,"1","null",*attValStr_tmp);
	    msiStrchop(*attValStr_tmp,*attValStr);
# if attName is equal to unit set to var and break for
            writeLine("serverLog", "        *varName:*attName = *attValStr");
# if varName is equal to "time", extract time:units for save Time Interval string (Ex.: "days since 1980-01-01")
	    if (*varName == "time" && *attName == "units" ){
	      *timeintervalstring = *attValStr;
	      writeLine("serverLog","Time Interval string = *timeintervalstring");
	    } 
	    if (*attName == "long_name" || *attName == "standard_name" ){
              *Str = "variable_" ++ *attName ++ "_" ++ *varName ++ "=*attValStr"
	      writeLine("serverLog","Add metadata string *Str to *ncFilePath");
              msiString2KeyValPair(*Str,*Keyval);
              msiAssociateKeyValuePairsToObj(*Keyval,*ncFilePath,"-d"); 
	    }
          }
         }
         *Str = "variable=*varName"
         writeLine("serverLog","Add metadata string *Str to *ncFilePath");
         msiString2KeyValPair(*Str,*Keyval);
         msiAssociateKeyValuePairsToObj(*Keyval,*ncFilePath,"-d");

# Get first and last value of time array
	 
	 if (*varName == "time") {
           if (msiNcGetVarIdInInqOut (*ncInqOut, *varName, *varId) != 0) {
             writeLine("serverLog", "msiNcGetVarIdInInqOut failed");
             fail;
           }
	   msiNcGetNdimsInInqOut (*ncInqOut, *varName, *ndims);
           for(*J=0;*J<*ndims;*J=*J+1) {
             if (msiNcGetDimLenInInqOut (*ncInqOut, *J, *varName, *dimLen) != 0) {
               writeLine("serverLog", "msiNcGetDimLenInInqOut failed");
               fail;
             }       
             msiAddToNcArray (0, *J, *startArray);
             msiAddToNcArray (*dimLen, *J, *countArray);
             msiAddToNcArray (1, *J, *strideArray);
           }
           if (msiNcGetVarsByType (*dataType, *ncid, *varId, *ndims, *startArray, *countArray, *strideArray, *getVarsOut) != 0) {
              writeLine("serverLog", "msiNcGetVarsByType failed");
           }   
           if (msiNcGetArrayLen (*getVarsOut, *varArrayLen) != 0) {
             writeLine("serverLog", "msiNcGetArrayLen failed"); 
           }  
	   *Kfirst = 0; 
           msiNcGetElementInArray (*getVarsOut, *Kfirst, *firstelement);
	   *Klast = *varArrayLen - 1; 
           msiNcGetElementInArray (*getVarsOut, *Klast, *lastelement);
           if (*dataType == 5) {
# float. writeLine cannot handle float yet.
             msiFloatToString (*firstelement, *firstfloatStr);
             msiFloatToString (*lastelement, *lastfloatStr);
           } 
	   else {
	     *firstfloatStr = "*firstelement";
	     *lastfloatStr = "*lastelement";
           }   
           writeLine("serverLog", "      *Kfirst: *firstfloatStr --- *Klast: *lastfloatStr");
           msiFreeNcStruct (*getVarsOut);
           msiFreeNcStruct (*startArray);
           msiFreeNcStruct (*countArray);
           msiFreeNcStruct (*strideArray);	   
# Call microservice msiCalcNcDates to calculate startdate and enddate of NetCDF file in format YYYYMMDDHHMM 
      if (msiCalcNcDates(*timeintervalstring, *firstfloatStr, *lastfloatStr, *startdate, *enddate ) == 0 )
         {
           writeLine("serverLog", "Startdate = *startdate --- Endadate = *enddate");
	   *Str = "startdate=*startdate"
           msiString2KeyValPair(*Str,*Keyval);
           msiAssociateKeyValuePairsToObj(*Keyval,*ncFilePath,"-d");
	   *Str = "enddate=*enddate"
           msiString2KeyValPair(*Str,*Keyval);
           msiAssociateKeyValuePairsToObj(*Keyval,*ncFilePath,"-d");
          }
           else {
             writeLine("serverLog", "msiCalcNcDates failed, no startdate and enddate AVUs inserted");
           }
	 }

       }

# copy *ncFilePath AVUs to parent dir
#       msiSplitPath(*ncFilePath,*Coll,*File);
#       *rs = select META_DATA_ATTR_NAME, META_DATA_ATTR_VALUE, META_DATA_ATTR_UNITS where DATA_NAME = '*File';
#       foreach(*row in *rs) {
#         *Str = *row.META_DATA_ATTR_NAME ++ "=" ++ *row.META_DATA_ATTR_VALUE
#         msiString2KeyValPair(*Str,*Keyval);
#         if (msiSetKeyValuePairsToObj(*Keyval,*Coll,"-C") == 0) {
#           writeLine("serverLog", "msiSetKeyValuePairsToObj AVUs to parent dir success");
#         } else {
#           writeLine("serverLog", "msiSetKeyValuePairsToObj AVUs to parent dir failed");
#           fail;
#         };
#         writeLine("serverLog", *row.META_DATA_ATTR_NAME ++ "=" ++ *row.META_DATA_ATTR_VALUE ++ " " ++ *row.META_DATA_ATTR_UNITS);
#       }

# close netcdf file
       msiNcClose(*ncid);
       writeLine("serverLog", "close file netcdf");
}



