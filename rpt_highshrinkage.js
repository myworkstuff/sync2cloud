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
async function process_salesdata(process_type,vartype,outletlist,period){
    let connPanda,connWeb,qry=''    
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        let item_list = await connWeb.query("select textlist from script_variables where setname='"+vartype+"'");
        item_list=item_list[0][0]['textlist'];
        let query = "SELECT location,`code`,a.itemcode,a.barcode,a.description, ";
        query += "CASE WHEN c.location='dcb' THEN IF(ISNULL(salesqty),0,salesqty) ELSE IF(ISNULL(salestempqty),0,salestempqty) END salestempqty, ";
        query += "CASE WHEN c.location='dcb' THEN IF(ISNULL(salesamt),0,salesamt) ELSE IF(ISNULL(salespos),0,salespos) END salespos ";
        query += "FROM ";
        query += "( ";
        query += "SELECT `code`,b.itemcode,packsize,barcode,b.description ";
        query += "FROM location a ";
        query += "LEFT JOIN ( ";
        query += "SELECT b.itemcode,barcode,b.description,packsize ";
        query += "FROM itemmaster b ";
        query += "LEFT JOIN itembarcode c ON b.itemcode=c.itemcode ";
        query += "WHERE b.itemcode IN("+item_list+") ";
        query += "GROUP BY b.itemcode ";
        query += ")b ON 1=1 WHERE `code` IN("+outletlist+") ";
        query += ")a ";
        query += "LEFT JOIN( ";
        query += "SELECT location,itemcode,salestempqty,salespos,salesqty,salesamt ";
        query += "FROM locationstock_period ";
        query += "WHERE location IN("+outletlist+") ";
        query += "AND periodcode='"+period+"' ";
        query += "AND itemcode IN("+item_list+") ";
        query += "GROUP BY location,itemcode ";
        query += ")c ON `code`=c.location AND a.itemcode=c.itemcode ";
        query += "GROUP BY `code`,a.itemcode ";
        query += "ORDER BY `code`,a.itemcode";
        let tbl_list = await connPanda.query(query);
        let tbllenght=tbl_list[0].length;
        for (let i = 0; i < tbl_list[0].length; i++) {  
            console.clear();console.log(process_type+' - '+i+'/'+tbllenght);            
            let tmp_Outlet=tbl_list[0][i]['location'];
            let tmp_itemcode=tbl_list[0][i]['itemcode'];
            let tmp_barcode=tbl_list[0][i]['barcode'];
            let tmp_description=tbl_list[0][i]['description'];
            tmp_description=tmp_description.replace(/'/g, "\\'");
            let tmp_PS=tbl_list[0][i]['PS'];
            let tmp_UP=tbl_list[0][i]['UP'];
            let tmp_salestempqty=tbl_list[0][i]['salestempqty'];
            let tmp_salespos=tbl_list[0][i]['salespos'];
            let tmp_docdate=tbl_list[0][i]['docdate'];            
            let tmp_qry ="UPDATE cksgroup_intra.tbl_highshrink SET salesqty="+tmp_salestempqty+",salesamt="+tmp_salespos+"";
            tmp_qry+=" WHERE itemcode='"+tmp_itemcode+"' and Outlet='"+tmp_Outlet+"' and salesmonth='"+period+"' and process_type='"+process_type+"'"            
            // console.log(tmp_qry);process.exit();
            await connWeb.query(tmp_qry);   
        }
    } catch (error) {                
        if (connWeb) {connWeb.rollback();}
        console.log(error)
    } finally {
      if (connPanda) {connPanda.release();}
      if (connWeb) {connWeb.release();}
    }         
}
async function process_adjusted(process_type,vartype,outletlist,period){
    let connPanda,connWeb,qry=''    
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        let item_list = await connWeb.query("select textlist from script_variables where setname='"+vartype+"'");
        item_list=item_list[0][0]['textlist'];
        let query = "SELECT"; 
        query += " `code` Outlet,a.itemcode,a.barcode,a.description,packsize PS,''UP,"; 
        query += " IF(ISNULL(c.QtyAdjusted),0,c.QtyAdjusted) QtyAdjusted,"; 
        query += " IF(ISNULL(c.AmountAdjusted),0,c.AmountAdjusted) AmountAdjusted, cast(docdate as char)docdate"; 
        query += " FROM("; 
        query += " SELECT `code`,b.itemcode,packsize,barcode,b.description"; 
        query += " FROM location a"; 
        query += " LEFT JOIN ("; 
        query += " SELECT b.itemcode, barcode, b.description, packsize"; 
        query += " FROM itemmaster b"; 
        query += " LEFT JOIN itembarcode c ON b.itemcode=c.itemcode"; 
        query += " WHERE b.itemcode IN("+item_list+")"; 
        query += " GROUP BY b.itemcode"; 
        query += " ) b ON 1=1"; 
        query += " WHERE `code` IN("+outletlist+")"; 
        query += " ) a"; 
        query += " LEFT JOIN("; 
        query += " SELECT Location, itemlink, itemcode, barcode, Description, docdate,"; 
        query += " IF(ISNULL(MAX(QtyAdjustedIN)),0,MAX(QtyAdjustedIN)) - IF(ISNULL(MAX(QtyAdjustedOUT)),0,MAX(QtyAdjustedOUT)) QtyAdjusted,"; 
        query += " IF(ISNULL(MAX(AmountAdjustedIN)),0,MAX(AmountAdjustedIN)) - IF(ISNULL(MAX(AmountAdjustedOUT)),0,MAX(AmountAdjustedOUT)) AmountAdjusted"; 
        query += " FROM("; 
        query += " SELECT Location, itemlink, itemcode, barcode, b.Description, docdate,"; 
        query += " CASE WHEN AdjType='OUT' THEN ROUND(SUM(qty),2) END QtyAdjustedOUT,"; 
        query += " CASE WHEN AdjType='IN' THEN ROUND(SUM(qty),2) END QtyAdjustedIN,"; 
        query += " CASE WHEN AdjType='OUT' THEN ROUND(SUM(totalprice),2) END AmountAdjustedOUT,"; 
        query += " CASE WHEN AdjType='IN' THEN ROUND(SUM(totalprice),2) END AmountAdjustedIN"; 
        query += " FROM backend.adjustmain a"; 
        query += " LEFT JOIN backend.adjustchild b ON a.refno=b.refno"; 
        query += " WHERE b.itemcode IN("+item_list+")"; 
        query += " AND location IN("+outletlist+")"; 
        query += " AND LEFT(docdate,7)='"+period+"'"; 
        query += " GROUP BY location, itemcode"; 
        query += " ORDER BY location, itemlink"; 
        query += " ) p"; 
        query += " GROUP BY location, itemcode"; 
        query += " ) c ON `code`=c.location AND a.itemcode=c.itemcode"; 
        query += " GROUP BY `code`, a.itemcode"; 
        query += " ORDER BY `code`, a.itemcode"; 
        let tbl_list = await connPanda.query(query);
        let tbllenght=tbl_list[0].length;
        for (let i = 0; i < tbl_list[0].length; i++) {  
            console.clear();console.log(process_type+' - '+i+'/'+tbllenght);
            let tmp_Outlet=tbl_list[0][i]['Outlet'];
            let tmp_itemcode=tbl_list[0][i]['itemcode'];
            let tmp_barcode=tbl_list[0][i]['barcode'];
            let tmp_description=tbl_list[0][i]['description'];
            tmp_description=tmp_description.replace(/'/g, "\\'");
            let tmp_PS=tbl_list[0][i]['PS'];
            let tmp_UP=tbl_list[0][i]['UP'];
            let tmp_QtyAdjusted=tbl_list[0][i]['QtyAdjusted'];
            let tmp_AmountAdjusted=tbl_list[0][i]['AmountAdjusted'];
            let tmp_docdate=tbl_list[0][i]['docdate'];if(await tmp_docdate!==null){tmp_docdate="'"+tmp_docdate+"'";}
            let tmp_qry ="REPLACE INTO cksgroup_intra.tbl_highshrink(process_type,outlet,itemcode,barcode,DESCRIPTION,ps,adjustedqty,adjustedamt,adjusteddate,salesqty,salesamt,salesmonth,last_process)";
            tmp_qry+=" values "
            tmp_qry+=" ('"+process_type+"','"+tmp_Outlet+"','"+tmp_itemcode+"','"+tmp_barcode+"','"+tmp_description+"','"+tmp_PS+"',"+tmp_QtyAdjusted+",'"+tmp_AmountAdjusted+"',"+tmp_docdate+",null,null,'"+period+"',now())";
            // console.log(tmp_qry);process.exit();
            await connWeb.query(tmp_qry);   
        }
        // await connWeb.commit();
    } catch (error) {                
        if (connWeb) {connWeb.rollback();}
        console.log(error)
    } finally {
      if (connPanda) {connPanda.release();}
      if (connWeb) {connWeb.release();}
    }         
}
console.clear();
async function process_sales(){    
    let FH = "'FABR','FBPT','FBPW','FBTR','FDSR','FGLP','FKPP','FKPS','FLBS','FMSG','FPGH','FPKR','FPLH','FPRG','FSAB','FSGM','FSIK','FSSL','FSTK','FSTP','FTKM','FTKP','FTMN','FTMS','FTMU','FTPT'";
    let SBH = "'BDS','DGO','KHM','KNG','M88','MGT','MLN','PPR','PR2','PTS','SJT','SPG','TLP','TRN','SBBK','SRGD','STLB','SLMW','SPRS','STPP','GBVL','GDGO','GDMP','GHTP','GINN','GINT','GKBT','GKMS','GKPP','GLTJ','GPLC','GPPR','GPTG','GT2A','GTWR'";
    let period='2024-08'
    // console.log('Processing Adjustment 2024 FH_TMP1');
    // await process_adjusted('2024 FH_TMP1','FH_TMP1',FH,period);
    // console.log('Adjustment 2024 FH_TMP1 - Done');
    // console.log('Processing Sales 2024 FH_TMP1');
    // await process_salesdata('2024 FH_TMP1','FH_TMP1',FH,period);
    // console.log('Sales 2024 FH_TMP1 - Done');

    // console.log('Processing Adjustment 2024 FH_TMP2');
    // await process_adjusted('2024 FH_TMP2','FH_TMP2',FH,period);
    // console.log('Adjustment 2024 FH_TMP2 - Done');
    // console.log('Processing Sales 2024 FH_TMP2');
    // await process_salesdata('2024 FH_TMP2','FH_TMP2',FH,period);
    // console.log('Sales 2024 FH_TMP2 - Done');
    
    // console.log('Processing Adjustment 2024 FH_TMP3');
    // await process_adjusted('2024 FH_TMP3','FH_TMP3',FH,period);
    // console.log('Adjustment 2024 FH_TMP3 - Done');
    // console.log('Processing Sales 2024 FH_TMP3');
    // await process_salesdata('2024 FH_TMP1','FH_TMP3',FH,period);
    // console.log('Sales 2024 FH_TMP3 - Done');

    // console.log('Processing Adjustment 2024 FH_TMP4');
    // await process_adjusted('2024 FH_TMP4','FH_TMP4',FH,period);
    // console.log('Adjustment 2024 FH_TMP4 - Done');
    // console.log('Processing Sales 2024 FH_TMP4');
    // await process_salesdata('2024 FH_TMP4','FH_TMP4',FH,period);
    // console.log('Sales 2024 FH_TMP4 - Done');

    console.log('Processing Adjustment 2024 SH_TMP1');
    await process_adjusted('2024 SH_TMP1','SH_TMP1',SBH,period);
    console.log('Adjustment 2024 SH_TMP1 - Done');
    console.log('Processing Sales 2024 SH_TMP1');
    await process_salesdata('2024 SH_TMP1','SH_TMP1',SBH,period);
    console.log('Sales 2024 SH_TMP1 - Done');

    console.log('Processing Adjustment 2024 SH_TMP2');
    await process_adjusted('2024 SH_TMP2','SH_TMP2',SBH,period);
    console.log('Adjustment 2024 SH_TMP2 - Done');
    console.log('Processing Sales 2024 SH_TMP2');
    await process_salesdata('2024 SH_TMP2','SH_TMP2',SBH,period);
    console.log('Sales 2024 SH_TMP2 - Done');

    console.log('Processing Adjustment 2024 SH_TMP3');
    await process_adjusted('2024 SH_TMP3','SH_TMP3',SBH,period);
    console.log('Adjustment 2024 SH_TMP3 - Done');
    console.log('Processing Sales 2024 SH_TMP3');
    await process_salesdata('2024 SH_TMP3','SH_TMP3',SBH,period);
    console.log('Sales 2024 SH_TMP3 - Done');

    // console.log('Processing Adjustment 2024 SH_TMP4');
    // await process_adjusted('2024 SH_TMP4','SH_TMP4',SBH,period);
    // console.log('Adjustment 2024 SH_TMP4 - Done');
    // console.log('Processing Sales 2024 SH_TMP4');
    // await process_salesdata('2024 SH_TMP4','SH_TMP4',SBH,period);
    // console.log('Sales 2024 SH_TMP4 - Done');

    process.exit();
}
process_sales();