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

async function process_transaction(bizdates){
    const tmpstamp = new Date();console.log('Populating sales : '+bizdates);
    let datas = [];
    let connPanda,connWeb,qry=''
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();    
        qry="SELECT * FROM cksgroup_intra.tbl_sales where bizdate='"+bizdates+"'";
        const tbl_location = await connWeb.query(qry); 
        for (let j = 0; j < tbl_location[0].length; j++) {
            const _location= tbl_location[0][j]['location'];
            const _bizdate= tbl_location[0][j]['bizdate'];
            const _category= tbl_location[0][j]['category'];                        
            let query = "";
            query += " SELECT *";
            query += " FROM frontend.poschild";
            query += " WHERE LEFT(bizdate,7)='"+_bizdate+"' AND location='"+_location+"' AND category='"+_category+"'";
            query += " GROUP BY refno";
            const [results1] = await connPanda.query(query);        
            const POSTrans =results1.length;          
            await connWeb.query("UPDATE cksgroup_intra.tbl_sales set pos_trans='"+POSTrans+"' WHERE LEFT(bizdate,7)='"+_bizdate+"' AND location='"+_location+"' AND category='"+_category+"'");
            console.log(_bizdate+ ' : '+_location+' '+_category+' '+POSTrans);  
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

async function process_data(){  
    console.log('Populating Start Daily Sales'); 
    const date = new Date(); 
    date.setDate(date.getDate() - 1);    
    const formattedYesterday = date.getFullYear() + '-' + String(date.getMonth() + 1).padStart(2, '0') + '-' + String(date.getDate()).padStart(2, '0');    
    const date2 = new Date(formattedYesterday); 
    const currentDay = date2.getDate(); 
    const currentWeekDay = date2.getDay(); 
    date2.setMonth(date2.getMonth() - 1);
    if (date2.getDate() !== currentDay) { date2.setDate(0); }
    let daysDifference = currentWeekDay - date2.getDay();        
    if (daysDifference < 0) { daysDifference += 7; }    
    date2.setDate(date2.getDate() + daysDifference);
    const formattedLastMonthSameDay = date2.getFullYear() + '-' + String(date2.getMonth() + 1).padStart(2, '0') + '-' + String(date2.getDate()).padStart(2, '0');

    let connPanda,connWeb
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();
        await connWeb.query("DELETE FROM cksgroup_intra.tbl_sales_daily");
        qry="SELECT `code` FROM location";
        const tbl_location = await connWeb.query(qry); 
        let query = "";      
        query += `
            SELECT 
            '${formattedYesterday}' bizdate,loc_group AS location,
            SUM(ROUND((amount),2)) AS today_sales,
            SUM((amount)-(qty*averagecost)) AS profit
            FROM frontend.poschild pc
            WHERE pc.bizdate ='${formattedYesterday}' AND void=0            
            GROUP BY pc.location
        `;     
        const [results] = await connPanda.query(query);    
        const insertPromises = results.map(row => {
          const columns = Object.keys(row).join(',');
          const values = Object.values(row).map(value => `'${detectDateType(value)}'`).join(',');
          return connWeb.query(`INSERT INTO cksgroup_intra.tbl_sales_daily (${columns}) VALUES (${values})`);
        });
        await Promise.all(insertPromises);
        query = "";
        query += `
            SELECT 
            cast(bizdate as char) lastmonth_date,loc_group AS location,
            SUM(ROUND((amount),2)) AS lastmonth_sales
            FROM frontend.poschild pc
            WHERE pc.bizdate ='${formattedLastMonthSameDay}' AND void=0            
            GROUP BY pc.location
        `;           
        const [results2] = await connPanda.query(query);    
        const insertPromises2 = results2.map(async row => {
            const lastmonth_date=row.lastmonth_date;
            const location=row.location;
            const lastmonth_sales=row.lastmonth_sales;
            connWeb.query("UPDATE cksgroup_intra.tbl_sales_daily set lastmonth_date='"+lastmonth_date+"',lastmonth_sales="+lastmonth_sales+",amt_achv=(today_sales-"+lastmonth_sales+") where location='"+location+"'");    
            return;
        });
        await Promise.all(insertPromises2);
        query = "";
        query += `
            SELECT 
            bizdate,loc_group AS location,SUM(ROUND((amount),2)) AS mtd_sales
            FROM frontend.poschild pc
            WHERE pc.bizdate between DATE_FORMAT(CURDATE(), '%Y-%m-01') and '${formattedYesterday}' AND void=0            
            GROUP BY pc.location
        `;                
        const [results3] = await connPanda.query(query);    
        const insertPromises3 = results3.map(async row => {
            const mtd_sales=row.mtd_sales;
            const location=row.location;            
            connWeb.query("UPDATE cksgroup_intra.tbl_sales_daily set mtd_sales='"+mtd_sales+"' where location='"+location+"'");
            return;
        });
        connWeb.query("UPDATE tbl_sales_daily SET amt_achv_100=(today_sales-lastmonth_sales)/today_sales WHERE today_sales>0");        
        await Promise.all(insertPromises3);
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
    process.exit();
}
process_sales();