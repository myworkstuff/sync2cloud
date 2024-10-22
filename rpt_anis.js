const mysql2 = require('mysql2/promise');
require("dotenv").config();
var bodyParser =require('body-parser');
const moment = require('moment');
const { format, startOfYear, startOfMonth, endOfMonth, addMonths } = require('date-fns');

const port = process.env.port;
const host = process.env.host;
const dbname = process.env.dbname;
const username = process.env.u_name;
const password = process.env.password;
let isget_lastsync=1

const dbpool= mysql2.createPool({
	host: host,port: port,user: username,password: password,database:dbname,
	waitForConnections: true,connectionLimit: 10,queueLimit: 0	
});
const dbpool2= mysql2.createPool({
	host: 'cksgroup.my',port: '3306',user: 'cksroot',password: '4h]k53&[ugwN',database:'cksgroup_intra',
	waitForConnections: true,connectionLimit: 10,queueLimit: 0	
});
function getFirstDecimalPointValue(num) {
    const numStr = num.toString();
    const [integerPart, decimalPart] = numStr.split('.');
    if (decimalPart && decimalPart.length > 0) {
        return parseInt(decimalPart[0], 10);
    }
    return 0;
}
function escapeSingleQuotes(str) {
    return str.replace(/'/g, "\\'");
}
function detectDateType(text){const dateFormat='YYYY-MM-DD',datetimeFormat='YYYY-MM-DDTHH:mm:ss.SSSZ';if(moment(text,datetimeFormat,true).isValid()){return moment(text).format('YYYY-MM-DD HH:mm:ss');}else if(moment(text,dateFormat,true).isValid()){return moment(text).format('YYYY-MM-DD');}else{return text;}}
async function process_data(code,itemcode_array,outlet,gtype){
    const tmpstamp = new Date();console.log('Populating Anis sales');
    let datas = [];
    let connPanda,connWeb,qry=''
    const startDate = startOfYear(new Date()); 
    const endDate = startOfMonth(new Date());    
    // console.log(gtype);process.exit();
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        for (let i = 0; i < outlet.length; i++) {  
            const _location=outlet[i];            
            let query = "";
            query += " SELECT '"+_location+"' location,a.itemcode,a.description,";
            query += " SUM(ROUND(IF(ISNULL(grn_qty),0,grn_qty),2))grn_qty,";
            query += " SUM(ROUND(IF(ISNULL(grn_price),0,grn_price),2))grn_price,";
            query += " SUM(ROUND(IF(ISNULL(return_amount),0,return_amount),2))return_amount,";
            query += " SUM(ROUND(IF(ISNULL(return_amountqty),0,return_amountqty),2))return_amountqty,";
            query += " SUM(ROUND(IF(ISNULL(post_amount),0,post_amount),2))post_amount,";
            query += " SUM(ROUND(IF(ISNULL(SoldQty),0,SoldQty),2))SoldQty,";
            query += " SUM(ROUND(IF(ISNULL(known_shrinkage),0,known_shrinkage),2))known_shrinkage,";
            query += " SUM(ROUND(IF(ISNULL(known_shrinkageqty),0,known_shrinkageqty),2))known_shrinkageqty,";
            query += " SUM(ROUND(IF(ISNULL(cn_amount),0,cn_amount),2))cn_amount,";
            query += " SUM(ROUND(IF(ISNULL(cn_cost),0,cn_cost),2))cn_cost,";
            query += " SUM(ROUND(IF(ISNULL(grn_qty),0,grn_qty)-IF(ISNULL(return_amountqty),0,return_amountqty)-IF(ISNULL(SoldQty),0,SoldQty)-IF(ISNULL(known_shrinkageqty),0,known_shrinkageqty),2))QOHqty,";
            query += " SUM(ROUND(IF(ISNULL(grn_price),0,grn_price)-IF(ISNULL(return_amount),0,return_amount)-IF(ISNULL(post_amount),0,post_amount)-IF(ISNULL(known_shrinkage),0,known_shrinkage),2))QOHPrice";
            query += " FROM backend.itemmaster a";
            query += " LEFT JOIN(";
            query += " SELECT a.location,itemcode,SUM(qty)grn_qty,SUM(totalprice)grn_price";
            query += " FROM backend.grmain a";
            query += " INNER JOIN backend.grchild b ON a.refno=b.refno";
            switch (gtype) {
                case 1:
                    query += " WHERE docdate =DATE(DATE_SUB(NOW(),INTERVAL 1 DAY))";
                break;
                case 2:
                case 4:                  
                    query += " WHERE docdate between DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) and DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY)";
                break;
                case 3:
                case 5:    
                    query += " WHERE docdate between DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY) - INTERVAL 1 MONTH and LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))";
                    query += " AND itemcode in("+itemcode_array+")";
                break;
                case 6:    
                    query += " WHERE docdate between LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 2 MONTH)) + INTERVAL 1 DAY and LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))";
                    query += " AND itemcode in("+itemcode_array+")";
                break;                
            }
            switch (gtype) {                
                case 5:    
                case 6:
                    query += " GROUP BY a.location";
                    query += " )b ON b.location='"+_location+"'";
                break;
                case 1:
                case 2:
                case 3:
                case 4:       
                    query += " GROUP BY itemcode,a.location";
                    query += " )b ON a.itemcode = b.itemcode AND b.location='"+_location+"'";
                break;                 
            }                        
            query += " LEFT JOIN(";
            //#DN - Return
            query += " SELECT a.location,itemcode,SUM(totalprice)return_amount,SUM(qty)return_amountqty";
            query += " FROM backend.dbnotemain a";
            query += " INNER JOIN backend.dbnotechild b ON a.refno=b.refno AND a.location=b.location";
            switch (gtype) {
                case 1:
                    query += " WHERE docdate =DATE(DATE_SUB(NOW(),INTERVAL 1 DAY)) AND a.location='"+_location+"'";
                break;
                case 2:
                case 4:
                    query += " WHERE docdate between DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) and DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AND a.location='"+_location+"'";
                break;
                case 3:
                case 5:
                    query += " WHERE docdate between DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY) - INTERVAL 1 MONTH AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))  AND a.location='"+_location+"'";
                    query += " AND itemcode in("+itemcode_array+")";
                break;
                case 6:
                    query += " WHERE docdate between LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 2 MONTH)) + INTERVAL 1 DAY and LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH)) AND a.location='"+_location+"'";
                    query += " AND itemcode in("+itemcode_array+")";
                break;                 
            }
            switch (gtype) {
                case 5:
                case 6:                    
                    query += " GROUP BY a.location";
                    query += " )c ON c.location='"+_location+"'";
                break;
                case 1:     
                case 2:
                case 3:
                case 4:                   
                    query += " GROUP BY a.location,itemcode";
                    query += " )c ON a.itemcode = c.itemcode AND c.location='"+_location+"'";
                break;                 
            }                                      
            query += " LEFT JOIN(";
            //#SALES
            query += " SELECT dept,subdept,category,location,itemcode,barcode,description,";
            query += " ROUND(SUM(amount),2)post_amount,";
            query += " ROUND(IF(SoldbyWeight=1,SUM(weightvalue),SUM(qty)),2)SoldQty";
            query += " FROM frontend.poschild";
            switch (gtype) {
                case 1:
                    query += " WHERE bizdate =DATE(DATE_SUB(NOW(),INTERVAL 1 DAY)) AND location ='"+_location+"'";
                break;
                case 2:
                case 4:
                    query += " WHERE bizdate between DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) and DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AND location='"+_location+"'";
                break;
                case 3:
                case 5:
                    query += " WHERE bizdate between DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY) - INTERVAL 1 MONTH AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))  AND location='"+_location+"'";
                    query += " AND itemcode in("+itemcode_array+")";
                break;                     
                case 6:
                    query += " WHERE bizdate between LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 2 MONTH)) + INTERVAL 1 DAY and LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH)) AND location='"+_location+"'";
                    query += " AND itemcode in("+itemcode_array+")";
                break;                   
            }                        
            switch (gtype) {
                case 5: 
                case 6:   
                    query += " GROUP BY location";
                    query += " )d ON d.location='"+_location+"'";
                break;
                case 1: 
                case 2:
                case 3:
                case 4:                       
                    query += " GROUP BY location,itemcode";
                    query += " )d ON a.itemcode = d.itemcode AND d.location='"+_location+"'";
                break;                 
            }                                     
            query += " LEFT JOIN(";
            //#Shrinkage Known
            query += " SELECT location,itemcode,ROUND(SUM(totalprice),2)known_shrinkage,ROUND(SUM(qty),2)known_shrinkageqty";
            query += " FROM backend.adjustmain a";
            query += " INNER JOIN backend.adjustchild b ON a.refno=b.refno";
            query += " INNER JOIN backend.set_master_code c ON c.code_desc=a.reason";            
            switch (gtype) {
                case 1:
                    query += " WHERE docdate=DATE(DATE_SUB(NOW(),INTERVAL 1 DAY)) AND location ='"+_location+"' AND trans_type='ADJUST_REASON' AND code_group='DISPOSAL' AND `type`='DISP'";
                break;
                case 2:
                case 4:
                    query += " WHERE docdate between DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) and DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AND location='"+_location+"' AND trans_type='ADJUST_REASON'  AND code_group='DISPOSAL' AND `type`='DISP'";
                break;
                case 3:
                case 5:
                    query += " WHERE docdate between DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY) - INTERVAL 1 MONTH AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))  AND location='"+_location+"' AND trans_type='ADJUST_REASON' AND code_group='DISPOSAL' AND `type`='DISP'";
                    query += " AND itemcode in("+itemcode_array+")";
                break;
                case 6:
                    query += " WHERE docdate between LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 2 MONTH)) + INTERVAL 1 DAY and LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH)) AND location='"+_location+"' AND trans_type='ADJUST_REASON' AND code_group='DISPOSAL' AND `type`='DISP'";
                    query += " AND itemcode in("+itemcode_array+")";
                break;                    
            }                         
            switch (gtype) {
                case 5:
                case 6:
                    query += " GROUP BY b.location";
                    query += " )e ON e.location='"+_location+"'";
                break;
                case 1:
                case 2:
                case 3:
                case 4:                     
                    query += " GROUP BY b.location,itemcode";
                    query += " )e ON a.itemcode = e.itemcode AND e.location='"+_location+"'";
                break;                 
            }
            query += " LEFT JOIN(";
            //#CN 
            query += " SELECT location,dept,subdept,category,itemcode,ROUND(SUM(totalprice),2)cn_amount,ROUND(SUM(lastcost*qty),2)cn_cost";
            query += " FROM backend.cnnotemain a";
            query += " INNER JOIN backend.cnnotechild b ON a.refno=b.refno";
            switch (gtype) {
                case 1:
                    query += " WHERE docdate =DATE(DATE_SUB(NOW(),INTERVAL 1 DAY)) AND a.location ='"+_location+"'";
                break;
                case 2:
                case 4:
                    query += " WHERE docdate between DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) and DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AND a.location='"+_location+"'";
                break;
                case 3:
                case 5:
                    query += " WHERE docdate between DATE_SUB(CURDATE(), INTERVAL DAYOFMONTH(CURDATE()) - 1 DAY) - INTERVAL 1 MONTH AND LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH))  AND a.location='"+_location+"'";
                    query += " AND itemcode in("+itemcode_array+")";
                break;
                case 6:
                    query += " WHERE docdate between LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 2 MONTH)) + INTERVAL 1 DAY and LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH)) AND a.location='"+_location+"'";
                    query += " AND itemcode in("+itemcode_array+")";
                break;                  
            }                        
            switch (gtype) {
                case 5:
                case 6:
                    query += " GROUP BY a.location";
                    query += " )f ON f.location='"+_location+"'";
                break;
                case 1:  
                case 2:
                case 3:
                case 4:                      
                    query += " GROUP BY a.location,itemcode";
                    query += " )f ON a.itemcode = f.itemcode AND f.location='"+_location+"'";
                break;                 
            }                                    
            query += " WHERE a.itemcode in("+itemcode_array+")";
            switch (gtype) {
                case 1:
                case 2:
                case 3:
                case 4:        
                    query += " GROUP BY a.itemcode";
                break;
            }            
            // console.log(query);process.exit();
            let tmp_table = await connPanda.query(query);              
            for (let j = 0; j < tmp_table[0].length; j++) {                 
                datas.push(tmp_table[0][j]);
            }
        }
            await connWeb.query("DELETE FROM tbl_datatables where code='"+code+"'");            
            const jsonString = JSON.stringify(datas);
            const escapedJsonString = escapeSingleQuotes(jsonString);               
            await connWeb.query("INSERT INTO tbl_datatables(code,payload)VALUE('"+code+"','"+escapedJsonString+"')");        
    } catch (error) {        
        if (connPanda) {connPanda.rollback();}
        if (connWeb) {connWeb.rollback();}
        console.log(error)
    } finally {
      if (connPanda) {connPanda.release();}
      if (connWeb) {connWeb.release();}
    }    
    console.log('Populating '+code+' Done');     
}
console.clear();
async function process_sales(){
    const Array_Vege="'12530','12537','12544','12551','12558','12593','12880','13881','14042','14063','14854','14861','15386','15393','15421','15428','15442','15449','15484'";
    let CKSR = ['BDS','DGO','KHM','KNG','M88','MGT','MLN','PPR','PR2','PTS','SJT','SPG','TLP','TRN'];
    let CKSM = ['GBVL','GDGO','GDMP','GHTP','GINN','GINT','GKBT','GKMS','GKPP','GLTJ','GPLC','GPPR','GPTG','GT2A','GTWR','STPP','STLB','SRGD','SPRS','SLMW','SBBK'];    
    // console.log('CKSR-Daily');
    // await process_data('CKSR-Daily g_sayur_anis',Array_Vege,CKSR,1);
    // console.log('CKSR-Daily - Done');
    // console.log('CKSM-Daily');
    // await process_data('CKSM-Daily g_sayur_anis',Array_Vege,CKSM,1);   
    // console.log('CKSM-Daily - Done');

    // console.log('CKSR-Weekly');
    // await process_data('CKSR-Weekly g_sayur_anis',Array_Vege,CKSR,2);
    // console.log('CKSR-Weekly - Done');
    // console.log('CKSM-Weekly');
    // await process_data('CKSM-Weekly g_sayur_anis',Array_Vege,CKSM,2);   
    // console.log('CKSM-Weekly - Done');    

    // console.log('CKSR-Weekly-summary');
    // await process_data('CKSR-Weekly-summary g_sayur_anis',Array_Vege,CKSR,4);   
    // console.log('CKSR-Weekly-summary - Done');  
    // console.log('CKSM-Weekly-summary');
    // await process_data('CKSM-Weekly-summary g_sayur_anis',Array_Vege,CKSM,4);   
    // console.log('CKSM-Weekly-summary - Done');  

    console.log('CKSR-Monthly');
    await process_data('CKSR-Monthly g_sayur_anis',Array_Vege,CKSR,3);
    console.log('CKSR-Monthly - Done');
    console.log('CKSM-Monthly');
    await process_data('CKSM-Monthly g_sayur_anis',Array_Vege,CKSM,3);   
    console.log('CKSM-Monthly - Done');    

    // console.log('CKSR-Monthly-summary');
    // await process_data('CKSR-Monthly-summary g_sayur_anis',Array_Vege,CKSR,5);   
    // console.log('CKSR-Monthly-summary - Done');  
    // console.log('CKSM-Monthly-summary');
    // await process_data('CKSM-Monthly-summary g_sayur_anis',Array_Vege,CKSM,5);   
    // console.log('CKSM-Monthly-summary - Done');  

    // console.log('CKSR-Monthly-summary-previuos');
    // await process_data('CKSR-Monthly-summary-previuos g_sayur_anis',Array_Vege,CKSR,6);   
    // console.log('CKSR-Monthly-summary-previuos - Done');  
    // console.log('CKSM-Monthly-summary-previuos');
    // await process_data('CKSM-Monthly-summary-previuos g_sayur_anis',Array_Vege,CKSM,6);   
    // console.log('CKSM-Monthly-summary-previuos - Done');  

    process.exit();
}
process_sales();