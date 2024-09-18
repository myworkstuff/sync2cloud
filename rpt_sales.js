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
async function process_data(bizdates){
    const tmpstamp = new Date();console.log('Populating sales : '+bizdates);
    let datas = [];
    let connPanda,connWeb,qry=''
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();
        await connWeb.query("DELETE FROM cksgroup_intra.tbl_sales where bizdate='"+bizdates+"'");
        qry="SELECT `code` FROM location WHERE LEFT(`code`,2)<>'DC' and `code`not in ('GDCF','GDC','HQ')";
        const tbl_location = await connWeb.query(qry); 
        for (let j = 0; j < tbl_location[0].length; j++) {
            const _location= tbl_location[0][j]['code'];
            
            let query = "";
            query = "SELECT '"+bizdates+"' bizdate,a.location,group_code div_id,group_desc div_disc, ";
            query += "b.dept dept_id,dept_desc deptdesc, ";
            query += "b.subdept,subdeptdesc,a.category,catdesc cat_desc, ";
            query += "a.pos_cost,a.post_amount, ";
            query += "ROUND(IF(ISNULL(si_cost),0,si_cost),2)si_cost, ";
            query += "ROUND(IF(ISNULL(si_amount),0,si_amount),2)si_amount, ";
            query += "ROUND(IF(ISNULL(cn_cost),0,cn_cost),2)cn_cost, ";
            query += "ROUND(IF(ISNULL(cn_amount),0,cn_amount),2)cn_amount, ";
            query += "ROUND(IF(ISNULL(known_shrinkage),0,known_shrinkage),2)known_shrinkage, ";
            query += "ROUND(IF(ISNULL(unknown_shrinkage),0,unknown_shrinkage),2)unknown_shrinkage, ";
            query += "trans pos_trans ";
            query += "FROM ";
            query += "( ";
            query += "   SELECT location,dept,subdept,category, ";
            query += "   ROUND(SUM(lastcost*qty),2)pos_cost, ";
            query += "   ROUND(SUM(amount),2)post_amount, ";
            query += "   COUNT(*)trans ";
            query += "   FROM frontend.poschild ";
            query += "   WHERE LEFT(bizdate,7)='"+bizdates+"' AND location='"+_location+"' ";
            query += "   GROUP BY location,category ";
            query += ")a ";
            query += "LEFT JOIN( ";
            query += "   SELECT IFNULL(a.group_code,'ZZ-NA') AS `group_code`,a.group_desc, ";
            query += "   dept_code dept,dept_desc,d.code subdept,d.description subdeptdesc, ";
            query += "   e.code category,e.description catdesc ";
            query += "   FROM set_group a ";
            query += "   INNER JOIN set_group_dept b ON a.group_code=b.group_code ";
            query += "   LEFT JOIN department c ON dept_code=c.code ";
            query += "   LEFT JOIN subdept d ON dept_code=d.mcode ";
            query += "   LEFT JOIN category e ON c.code=e.deptcode AND d.code=e.mcode ";
            query += "   GROUP BY e.code ";
            query += "   ORDER BY a.group_code,e.code ";
            query += ")b ON a.category=b.category ";
            query += "LEFT JOIN( ";
            query += "   SELECT location,dept,subdept,category,SUM(lastcost*qty)si_cost,SUM(totalprice)si_amount ";
            query += "   FROM backend.simain a ";
            query += "   INNER JOIN backend.sichild b ON a.refno=b.refno ";
            query += "   WHERE LEFT(InvoiceDate,7)='"+bizdates+"' AND location='"+_location+"' ";
            query += "   GROUP BY location,category ";
            query += ")c ON a.category=c.category AND c.location=a.location ";
            query += "LEFT JOIN( ";
            query += "   SELECT location,dept,subdept,category,ROUND(SUM(totalprice),2)cn_amount,ROUND(SUM(lastcost*qty),2)cn_cost ";
            query += "   FROM backend.cnnotemain a ";
            query += "   INNER JOIN backend.cnnotechild b ON a.refno=b.refno ";
            query += "   WHERE LEFT(docdate,7)='"+bizdates+"' AND a.location='"+_location+"' ";
            query += "   GROUP BY location,category ";
            query += ")e ON a.category=e.category AND e.location=a.location ";
            query += "LEFT JOIN( ";
            query += "   SELECT location,dept,subdept,category,ROUND(SUM(totalprice)*-1,2)known_shrinkage ";
            query += "   FROM backend.adjustmain a ";
            query += "   INNER JOIN backend.adjustchild b ON a.refno=b.refno ";
            query += "   WHERE LEFT(docdate,7)='"+bizdates+"' AND location = '"+_location+"' AND `type`='DISP' ";
            query += "   GROUP BY b.location,category ";
            query += ")f ON a.category=f.category AND f.location=a.location ";
            query += "LEFT JOIN( ";
            query += "   SELECT location,category,dept,subdept, ";
            query += "   COALESCE(MAX(ins), 0) - COALESCE(MAX(outs), 0)unknown_shrinkage, ";
            query += "   COALESCE(MAX(qtyins), 0) - COALESCE(MAX(qtyouts), 0)unknown_shrinkageqty ";
            query += "   FROM( ";
            query += "       SELECT location,adjtype,`type`,dept,subdept,category, ";
            query += "       CASE WHEN adjtype='in' THEN ROUND(SUM(totalprice),2) END ins, ";
            query += "       CASE WHEN adjtype='out' THEN ROUND(SUM(totalprice),2) END outs, ";
            query += "       CASE WHEN adjtype='in' THEN ROUND(SUM(qty),2) END qtyins, ";
            query += "       CASE WHEN adjtype='out' THEN ROUND(SUM(qty),2) END qtyouts ";
            query += "       FROM backend.adjustmain a ";
            query += "       INNER JOIN backend.adjustchild b ON a.refno=b.refno ";
            query += "       WHERE LEFT(docdate,7)='"+bizdates+"' AND location='"+_location+"' ";
            query += "       AND `type`<>'DISP' ";
            query += "       GROUP BY location,adjtype,`type`,category ";
            query += "   )a ";
            query += "   GROUP BY location,category ";
            query += ")g ON a.category=g.category AND g.location=a.location ";
            query += "GROUP BY a.category ";
            query += "ORDER BY group_code,subdept,a.category ASC";            

            // console.log(query);process.exit();
            const [results] = await connPanda.query(query);    
            const insertPromises = results.map(row => {
              const columns = Object.keys(row).join(',');
              const values = Object.values(row).map(value => `'${detectDateType(value)}'`).join(',');
              return connWeb.query(`INSERT IGNORE INTO cksgroup_intra.tbl_sales (${columns}) VALUES (${values})`);
            });            
            await Promise.all(insertPromises);
            console.log(bizdates+ ' : '+_location);  
        }
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
    await process_data('2023-12');
    await process_data('2023-11');
    await process_data('2023-10');
    await process_data('2023-09');
    await process_data('2023-08');
    await process_data('2023-07');    
    await process_data('2023-06');
    await process_data('2023-05');
    await process_data('2023-04');
    await process_data('2023-03');
    await process_data('2023-02');
    await process_data('2023-01');
    console.log('Done');
    process.exit();
}
process_sales();