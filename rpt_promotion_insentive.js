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
function detectDateType(text){const dateFormat='YYYY-MM-DD',datetimeFormat='YYYY-MM-DDTHH:mm:ss.SSSZ';if(moment(text,datetimeFormat,true).isValid()){return moment(text).format('YYYY-MM-DD HH:mm:ss');}else if(moment(text,dateFormat,true).isValid()){return moment(text).format('YYYY-MM-DD');}else{return text;}}
function escapeSingleQuotes(str) {
    return str.replace(/'/g, "\\'");
}
async function process_data(){
    const tmpstamp = new Date();console.log('Processing Promotion Insentive');
    let queries = [];
    let connPanda,connWeb,qry=''
    const startDate = startOfYear(new Date()); 
    const endDate = startOfMonth(new Date());    
    // const finalQuery = await generateQueries(startDate, endDate, location, barcode);
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();        
        let query =" SELECT supcode,supname,outlet,division,display_name,refno,FORMAT(SUM(amount),2) AS Total, FORMAT(SUM(amount_01),2) AS 'Jan',FORMAT(SUM(amount_02),2) AS 'Feb',FORMAT(SUM(amount_03),2) AS 'Mar',FORMAT(SUM(amount_04),2) AS 'Apr', FORMAT(SUM(amount_05),2) AS 'May',FORMAT(SUM(amount_06),2) AS 'Jun',FORMAT(SUM(amount_07),2) AS 'Jul',FORMAT(SUM(amount_08),2) AS 'Aug', FORMAT(SUM(amount_09),2) AS 'Sep',FORMAT(SUM(amount_10),2) AS 'Oct',FORMAT(SUM(amount_11),2) AS 'Nov',FORMAT(SUM(amount_12),2) AS 'Dec' FROM(  SELECT supcode,a.refno,supname,PERIOD,SUM(paidamount) AS amount,display_type,  IF(RIGHT(PERIOD,2)='01',SUM(paidamount),0) AS amount_01,IF(RIGHT(PERIOD,2)='02',SUM(paidamount),0) AS amount_02,  IF(RIGHT(PERIOD,2)='03',SUM(paidamount),0) AS amount_03,IF(RIGHT(PERIOD,2)='04',SUM(paidamount),0) AS amount_04,  IF(RIGHT(PERIOD,2)='05',SUM(paidamount),0) AS amount_05,IF(RIGHT(PERIOD,2)='06',SUM(paidamount),0) AS amount_06,  IF(RIGHT(PERIOD,2)='07',SUM(paidamount),0) AS amount_07,IF(RIGHT(PERIOD,2)='08',SUM(paidamount),0) AS amount_08,  IF(RIGHT(PERIOD,2)='09',SUM(paidamount),0) AS amount_09,IF(RIGHT(PERIOD,2)='10',SUM(paidamount),0) AS amount_10,  IF(RIGHT(PERIOD,2)='11',SUM(paidamount),0) AS amount_11,IF(RIGHT(PERIOD,2)='12',SUM(paidamount),0) AS amount_12,  division,dept,deptdesc,subdept,subdeptdesc,outlet,display_name FROM(  SELECT LEFT(paymentdate,7) AS PERIOD,a.refno,a.code AS supcode,a.name AS supname,paidamount,displaytype AS display_type,  CONCAT(a.displaytype,' - ',d.description) AS display_name,c.group_desc AS division,dept,deptdesc,subdept,subdeptdesc,loc_group AS outlet  FROM backend.dischememain a  INNER JOIN dischemechild b ON a.refno=b.refno  INNER JOIN(SELECT NAME,DESCRIPTION FROM backend.acc_code GROUP BY NAME ) d ON a.displaytype=d.NAME  INNER JOIN `view_subdept_div` c ON a.subdeptcode=c.subdept WHERE a.billstatus=1 AND LEFT(paymentdate,7) BETWEEN '2024-01' AND '2024-12' AND canceled=0 AND LEFT(loc_group,2) NOT IN ('dc','hq'))a GROUP BY outlet,PERIOD,subdept,supcode,refno,display_type)c GROUP BY outlet,subdept,supcode,refno,display_type ORDER BY supcode";
        let tmp_tables2 = await connPanda.query(query);  
        const jsonString = JSON.stringify(tmp_tables2[0]);
        const escapedJsonString = escapeSingleQuotes(jsonString);        
        await connWeb.query("REPLACE INTO tbl_datatables(code,payload)VALUE('promotion_insentive','"+escapedJsonString+"')");
    } catch (error) {        
        if (connPanda) {connPanda.rollback();}
        if (connWeb) {connWeb.rollback();}
        console.log(error)
    } finally {
      if (connPanda) {connPanda.release();}
      if (connWeb) {connWeb.release();}
    }        
    console.log('Done');
    process.exit();
}
process_data();