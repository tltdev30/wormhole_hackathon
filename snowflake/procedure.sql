-- Create proc to load data from external stage into table
CREATE OR REPLACE PROCEDURE WORMHOLE.INGESTION.COPY_SP(TABLE_NAME varchar)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$

   
    function twoDigit(n) { return (n < 10 ? '0' : '') + n; }
    var now = new Date(new Date().getTime() + (15 * 60 * 60 * 1000));
    currentDate = now.getFullYear() + twoDigit(now.getMonth() + 1) + twoDigit(now.getDate());
    var pattern = ".*" + TABLE_NAME.split('.').pop() + "/" + currentDate + ".*"
    var return_rows = [];

    var sql_cmd = "copy into " + TABLE_NAME + " from @wormhole.INGESTION.s3_stage pattern='" + pattern + "' FORCE = TRUE; ";
    var sql_stmt = snowflake.createStatement({sqlText: sql_cmd});
    var result = sql_stmt.execute();
    result.next();
    return_rows.push(result.getColumnValue(1))
    
    return return_rows;
        
$$;

call INGESTION.COPY_SP('INGESTION.EMPLOYEES');
