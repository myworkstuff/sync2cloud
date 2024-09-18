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
function detectDateType(text){
    const dateFormat='YYYY-MM-DD',datetimeFormat='YYYY-MM-DDTHH:mm:ss.SSSZ';
    if(moment(text,datetimeFormat,true).isValid()){
        return moment(text).format('YYYY-MM-DD HH:mm:ss');
    }else if(moment(text,dateFormat,true).isValid()){
        return moment(text).format('YYYY-MM-DD');
        }else{
            if(text=='Invalid Date'){text='0000-00-00 00:00:00'}
            if (typeof text === 'string') {text = text.replace(/\\/g, "");text = text.replace(/'/g, "\\'");}            
            return text;
        }
}
function escapeSingleQuotes(str) {
    return str.replace(/'/g, "\\'");
}
async function process_data(){
    const tmpstamp = new Date();console.log('Populating sales : ');
    let datas = [];
    let connPanda,connWeb,qry=''
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();
        await connWeb.query("DELETE FROM cksgroup_intra.tbl_datatables where code='rpt_supp_po'");
        qry="SELECT `code` FROM location WHERE LEFT(`code`,2)<>'DC' and `code`not in ('GDCF','GDC','HQ')";
        const tbl_location = await connWeb.query(qry);         
        for (let j = 0; j < tbl_location[0].length; j++) {
            const _location= tbl_location[0][j]['code'];
            let qry="SELECT z.location,z.scode,z.sname,z.po 'TotalPOLastYear',";
            qry+="CASE WHEN z.po >=12 THEN ROUND(z.po/12,2) ELSE 0 END po_Monthly,";
            qry+="CASE WHEN ROUND(z.po/12,2)>= 30 THEN ROUND(ROUND(z.po/12,2)/4,2) ELSE 0 END po_Weekly,";
            qry+="CASE WHEN ROUND(z.po/12,2)>= 30 THEN ROUND(ROUND(z.po/12,2)/30,2) ELSE 0 END po_Daily,";
            qry+="SUM(IF(ISNULL(d.po),0,d.po)) Cur_yearpo,";
            qry+="SUM(IF(ISNULL(c.po),0,c.po)) Cur_monthpo,";
            qry+="SUM(IF(ISNULL(b.po),0,b.po)) Cur_weekpo,";
            qry+="SUM(IF(ISNULL(e.po),0,e.po)) Cur_Day ";
            qry+="FROM(";
            qry+="SELECT scode,sname,location,COUNT(*)po ";
            qry+="FROM pomain WHERE location='"+_location+"' AND completed=1 ";
            qry+="AND podate BETWEEN DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 YEAR),'%Y-01-01') AND DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 YEAR),'%Y-12-31') ";
            qry+="GROUP BY scode";
            qry+=")z ";
            qry+="LEFT JOIN (";
            qry+="SELECT scode,sname,location,COUNT(*)po ";
            qry+="FROM pomain WHERE location='"+_location+"' AND completed=1 ";
            qry+="AND podate BETWEEN DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE())) DAY) AND DATE_ADD(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY), INTERVAL 6 DAY) ";
            qry+="GROUP BY scode";
            qry+=") b ON z.scode=b.scode AND z.location=b.location ";
            qry+="LEFT JOIN (";
            qry+="SELECT scode,sname,location,COUNT(*)po ";
            qry+="FROM pomain WHERE location='"+_location+"' AND completed=1 ";
            qry+="AND podate BETWEEN DATE_FORMAT(CURDATE(), '%Y-%m-01') AND DATE_FORMAT(CURDATE(), '%Y-%m-31') ";
            qry+="GROUP BY scode";
            qry+=")c ON z.scode=c.scode AND z.location=c.location ";
            qry+="LEFT JOIN (";
            qry+="SELECT scode,sname,location,COUNT(*)po ";
            qry+="FROM pomain WHERE location='"+_location+"' AND completed=1 ";
            qry+="AND podate BETWEEN DATE_FORMAT(CURDATE(), '%Y-01-01') AND DATE_FORMAT(CURDATE(), '%Y-12-31') ";
            qry+="GROUP BY scode";
            qry+=")d ON z.scode=d.scode AND z.location=d.location ";
            qry+="LEFT JOIN (";
            qry+="SELECT scode,sname,location,COUNT(*)po ";
            qry+="FROM pomain WHERE location='"+_location+"' AND completed=1 ";
            qry+="AND podate = DATE(NOW()) ";
            qry+="GROUP BY scode";
            qry+=")e ON z.scode=e.scode AND z.location=e.location ";
            qry+="GROUP BY z.scode";                        
            let tmp_table = await connPanda.query(qry); 
            for (let j = 0; j < tmp_table[0].length; j++) {                 
                datas.push(tmp_table[0][j]);
            }                         
        }
        const jsonString = JSON.stringify(datas);
        const escapedJsonString = escapeSingleQuotes(jsonString);                     
        await connWeb.query("INSERT INTO tbl_datatables(code,payload)VALUE('rpt_supp_po','"+escapedJsonString+"')");
        await connWeb.commit();  
    } catch (error) {        
        if (connPanda) {connPanda.rollback();}
        if (connWeb) {connWeb.rollback();}
        console.log(error)
    } finally {
      if (connPanda) {connPanda.release();}
      if (connWeb) {connWeb.release();}
    }    
    console.log('Populating  Done');     
}
console.clear();
async function process_sales(){
    await process_data();   
    console.log('Done');
    process.exit();
}
process_sales();