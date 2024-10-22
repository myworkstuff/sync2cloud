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

async function process_data(MonDate,SunDate,weekNumber,LastMonthmonday,LastMonthsunday){
    console.log('Populating weekly sales : ');
    const date = new Date();
    date.setDate(date.getDate() - 1);    
    const formattedYesterday = date.getFullYear() + '-' +      String(date.getMonth() + 1).padStart(2, '0') + '-' +      String(date.getDate()).padStart(2, '0');        
    const date2 = new Date();
    const currentDay = date2.getDate();
    const currentWeekDay = date2.getDay();
    date2.setMonth(date2.getMonth() - 1);
    if (date2.getDate() !== currentDay) {date2.setDate(0);}
    const daysDifference = currentWeekDay - date2.getDay();
    date2.setDate(date2.getDate() + daysDifference);
    const formattedLastMonthSameDay = date2.getFullYear() + '-' +  String(date2.getMonth() + 1).padStart(2, '0') + '-' +  String(date2.getDate()).padStart(2, '0');    
    let connPanda,connWeb
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();        
        qry="SELECT `code` FROM location";
        const tbl_location = await connWeb.query(qry); 
        if(await tbl_location[0].length>0){
            for (let j = 0; j < tbl_location[0].length; j++) {
                const _location= tbl_location[0][j]['code'];  
                console.log('Processing : '+_location);
                // await connWeb.query("DELETE FROM cksgroup_intra.tbl_sales_weekly");
                let query = "";      
                query += `
                    SELECT 
                    '${weekNumber}'weekNumber,left('${SunDate}',7)month_sales,
                    '${MonDate} and ${SunDate}' bizdate,loc_group AS location,
                    ROUND(SUM(ROUND((amount),2)),2) AS today_sales,
                    ROUND(SUM((amount)-(qty*(lastcost))),2) AS profit
                    FROM frontend.poschild pc
                    WHERE pc.bizdate between '${MonDate}' and '${SunDate}' AND void=0   
                    AND pc.location='${_location}' 
                    GROUP BY pc.location
                `;     
                const [results] = await connPanda.query(query);    
                const insertPromises = results.map(row => {
                  const columns = Object.keys(row).join(',');
                  const values = Object.values(row).map(value => `'${detectDateType(value)}'`).join(',');
                  return connWeb.query(`REPLACE INTO cksgroup_intra.tbl_sales_weekly (${columns}) VALUES (${values})`);
                });
                await Promise.all(insertPromises);
                query = "";
                query += `
                    SELECT 
                    bizdate,loc_group AS location,
                    SUM(ROUND((amount),2)) AS lastmonth_sales
                    FROM frontend.poschild pc
                    WHERE pc.bizdate between '${LastMonthmonday}' and '${LastMonthsunday}'  AND void=0    
                    AND pc.location='${_location}'
                    GROUP BY pc.location
                `;           
                const [results2] = await connPanda.query(query);    
                const insertPromises2 = results2.map(async row => {
                    const lastmonth_date=row.lastmonth_date;
                    const location=row.location;
                    const lastmonth_sales=row.lastmonth_sales;
                    connWeb.query("UPDATE cksgroup_intra.tbl_sales_weekly set lastmonth_date='"+LastMonthmonday+" and "+LastMonthsunday+"',lastmonth_sales="+lastmonth_sales+",amt_achv=(today_sales-"+lastmonth_sales+") where location='"+location+"' and weekNumber='"+weekNumber+"' and bizdate='"+MonDate+" and "+SunDate+"'");    
                    return;
                });
                await Promise.all(insertPromises2);               
                connWeb.query("UPDATE tbl_sales_weekly SET amt_achv_100=(today_sales-lastmonth_sales)/today_sales WHERE today_sales>0");
            }
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
function getWeekNumberOfMonth(date) {
    const startOfMonth = new Date(date.getFullYear(), date.getMonth(), 1); 
    const startOfWeek = startOfMonth.getDay(); 
    const dayOfMonth = date.getDate();     
    const adjustedStartOfWeek = (startOfWeek + 6) % 7;     
    const adjustedDay = dayOfMonth + adjustedStartOfWeek; 
    const weekNumber = Math.ceil(adjustedDay / 7);     
    return weekNumber;
}
function getLastWeekDates(date) {
    const today = date ? new Date(date) : new Date(); 
    const dayOfWeek = today.getDay(); 
    const lastWeekMonday = new Date(today);
    const daysToMonday = (dayOfWeek + 6) % 7 + 7;
    lastWeekMonday.setDate(today.getDate() - daysToMonday); 
    const lastWeekSunday = new Date(lastWeekMonday);     
    lastWeekSunday.setDate(lastWeekMonday.getDate() + 6);     
    const weekNumber = getWeekNumberOfMonth(new Date(lastWeekSunday));
    const formattedMonday = lastWeekMonday.toISOString().slice(0, 10);
    const formattedSunday = lastWeekSunday.toISOString().slice(0, 10);
    return {
        lastMonday: formattedMonday,lastSunday: formattedSunday,weekNumber:weekNumber
    }; 
}
function getLastMonthDaysByWeek(weekNumber) {
    const today = new Date();
    const lastMonth = today.getMonth() === 0 ? 11 : today.getMonth() - 1;
    const lastMonthYear = today.getMonth() === 0 ? today.getFullYear() - 1 : today.getFullYear();    

    const firstDayOfLastMonth = new Date(lastMonthYear, lastMonth, 1);    
    const firstMondayOfLastMonth = new Date(firstDayOfLastMonth);
    firstMondayOfLastMonth.setDate(firstDayOfLastMonth.getDate() + (1 - firstDayOfLastMonth.getDay() + 7) % 6);

    const targetMonday = new Date(firstMondayOfLastMonth);
    targetMonday.setDate(firstMondayOfLastMonth.getDate() + (weekNumber - 1) * 7);

    const targetSunday = new Date(targetMonday);
    targetSunday.setDate(targetMonday.getDate() + 6);

    return {
        LastMonthmonday: targetMonday.toISOString().split('T')[0],
        LastMonthsunday: targetSunday.toISOString().split('T')[0]
    };
}
async function process_sales(){
    console.clear();
    const weekDates = getLastWeekDates('2024-10-07');
    console.log(weekDates);
    const lastMontthweekDates = getLastMonthDaysByWeek(weekDates.weekNumber);
    console.log(lastMontthweekDates);
    // process.exit();
    await process_data(weekDates.lastMonday,weekDates.lastSunday,weekDates.weekNumber,lastMontthweekDates.LastMonthmonday,lastMontthweekDates.LastMonthsunday)       
    console.log('Done');
    process.exit();
}
process_sales();