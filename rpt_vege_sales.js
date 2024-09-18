const mysql2 = require('mysql2/promise');
require("dotenv").config();
var bodyParser =require('body-parser');
const moment = require('moment');

const port = process.env.port;
const host = process.env.host;
const dbname = process.env.dbname;
const username = process.env.u_name;
const password = process.env.password;
let isget_lastsync=1
const { subMonths, getDaysInMonth, setDate, format } = require('date-fns');


const dbpool= mysql2.createPool({
	host: host,port: port,user: username,password: password,database:dbname,
	waitForConnections: true,connectionLimit: 10,queueLimit: 0	
});
const dbpool2= mysql2.createPool({
	host: 'cksgroup.my',port: '3306',user: 'cksroot',password: '4h]k53&[ugwN',database:'cksgroup_intra',
	waitForConnections: true,connectionLimit: 10,queueLimit: 0	
});
async function get_table_schema(table_name,database_name,conn){try{let field_name="",qry="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='"+database_name+"' AND TABLE_NAME='"+table_name+"'",tbldata=await conn.query(qry);for(let i=0;i<tbldata[0].length;i++)field_name+=tbldata[0][i]['COLUMN_NAME']+",";return field_name=field_name.slice(0,-1),field_name}catch(error){return console.log(error),!1}}
function getFirstDecimalPointValue(num) {
    const numStr = num.toString();
    const [integerPart, decimalPart] = numStr.split('.');
    if (decimalPart && decimalPart.length > 0) {
        return parseInt(decimalPart[0], 10);
    }
    return 0;
}
function detectDateType(text){const dateFormat='YYYY-MM-DD',datetimeFormat='YYYY-MM-DDTHH:mm:ss.SSSZ';if(moment(text,datetimeFormat,true).isValid()){return moment(text).format('YYYY-MM-DD HH:mm:ss');}else if(moment(text,dateFormat,true).isValid()){return moment(text).format('YYYY-MM-DD');}else{return text;}}

function getLastMonthSameDate(date) {
    const previousMonthDate = subMonths(date, 1);
    const daysInPreviousMonth = getDaysInMonth(previousMonthDate);    
    const adjustedDate = setDate(previousMonthDate, Math.min(date.getDate(), daysInPreviousMonth));
    return adjustedDate;
}
const getDateOfWeekday = (dateString, dayOfWeek) => {
    const date = new Date(dateString);
    const currentDay = date.getDay();
    const difference = (dayOfWeek - currentDay + 7) % 7;
    date.setDate(date.getDate() + difference);
    return date;
};
// const getNextWeekDay = (dateString,dayOfWeek) => {
//     const today = new Date(dateString);
//     const currentDay = today.getDay();
//     const daysUntilNextWeek = (7 - currentDay + dayOfWeek) % 7 + 7;
//     today.setDate(today.getDate() + daysUntilNextWeek);
//     return today;
// };
const getNextWeekDay = (dateString, dayOfWeek) => {
    const today = new Date(dateString);
    const currentDay = today.getDay();        
    let daysUntilNextWeek = (dayOfWeek - currentDay + 7) % 7;        
    if (daysUntilNextWeek === 0) {
        daysUntilNextWeek += 7;
    }    
    today.setDate(today.getDate() + daysUntilNextWeek);
    return today.toISOString().split('T')[0];
};
async function customRound(number) {
    const integerPart = Math.floor(number);
    const decimalPart = number - integerPart;

    if (decimalPart >= 0.35 && decimalPart < 0.5) {
        return integerPart + 0.5;
    } else if (decimalPart >= 0.6 && decimalPart <= 0.9) {
        return Math.ceil(number);
    } else {
        return Math.floor(number);
    }
}
async function create_qry(currentTue,_location,_barcode){        
    let query = "";
    query += "SELECT b.location,aa.barcode,a.description,";
    query += "IF(ISNULL(SoldQty),0,SoldQty) SoldQty,";
    query += "IF(ISNULL(grn_qty),0,grn_qty) grn_qty ";
    query += "FROM itemmaster a ";
    query += "INNER JOIN itembarcode aa ON a.itemcode = aa.itemcode ";
    query += "INNER JOIN ( ";
    query += "SELECT bizdate, itemcode, location,";
    query += "ROUND(SUM(amount),2) post_amount, ";
    query += "ROUND(IF(SoldbyWeight=1,SUM(weightvalue),SUM(qty))) SoldQty ";
    query += "FROM frontend.poschild ";
    query += "WHERE bizdate = '"+currentTue+"'";
    query += "GROUP BY bizdate, location, itemcode ";
    query += ") b ON a.itemcode = b.itemcode ";
    query += "LEFT JOIN ( ";
    query += "SELECT docdate, a.location, itemcode, SUM(qty) grn_qty ";
    query += "FROM backend.grmain a ";
    query += "INNER JOIN backend.grchild b ON a.refno = b.refno ";
    query += "WHERE docdate = '"+currentTue+"'";
    query += "GROUP BY docdate, itemcode, a.location ";
    query += ") c ON a.itemcode = c.itemcode AND b.bizdate = c.docdate AND b.location = c.location ";
    query += "WHERE barcode IN("+_barcode+") ";
    query += "GROUP BY aa.barcode, b.location";
    return query;
}
async function process_data(salestype,Array_barcode,sales_period){    
    let connPanda,connWeb,qry=''        
    const today = new Date('2024-08-27');
    const dayOfWeek = today.getDay();
    const diffToTue = (dayOfWeek === 0 ? -6 : 2) - dayOfWeek;
    const diffToWed = (dayOfWeek === 0 ? -6 : 3) - dayOfWeek;
    const diffToThu = (dayOfWeek === 0 ? -6 : 4) - dayOfWeek;
    const diffToFri = (dayOfWeek === 0 ? -6 : 5) - dayOfWeek;
    const diffToSat = (dayOfWeek === 0 ? -6 : 6) - dayOfWeek;
    const diffToSun = (dayOfWeek === 0 ? -6 : 7) - dayOfWeek;
    const diffToMon = (dayOfWeek === 0 ? -6 : 8) - dayOfWeek;
    let currentTue = new Date(today);currentTue.setDate(today.getDate() + diffToTue);currentTue = currentTue.toISOString().split('T')[0];
    let currentWed = new Date(today);currentWed.setDate(today.getDate() + diffToWed);currentWed = currentWed.toISOString().split('T')[0];
    let currentThu = new Date(today);currentThu.setDate(today.getDate() + diffToThu);currentThu = currentThu.toISOString().split('T')[0];
    let currentFri = new Date(today);currentFri.setDate(today.getDate() + diffToFri);currentFri = currentFri.toISOString().split('T')[0];
    let currentSat = new Date(today);currentSat.setDate(today.getDate() + diffToSat);currentSat = currentSat.toISOString().split('T')[0];
    let currentSun = new Date(today);currentSun.setDate(today.getDate() + diffToSun);currentSun = currentSun.toISOString().split('T')[0];
    let currentMon = new Date(today);currentMon.setDate(today.getDate() + diffToMon);currentMon = currentMon.toISOString().split('T')[0];
    // console.log(currentThu)
    
    // process.exit()       
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();
        
        let tpmqry_="update custom_sales_bck set tue_grn=0, tue_sales=0,wed_grn=0, wed_sales=0,thu_grn=0, thu_sales=0,fri_grn=0, fri_sales=0,sat_grn=0, sat_sales=0,sun_grn=0, sun_sales=0,mon_grn=0, mon_sale=0 where salestype='"+salestype+"' and sales_period='"+sales_period+"'";
        await connWeb.query(tpmqry_);console.log(currentTue);
        let qry= await create_qry(currentTue,'',Array_barcode);
        let tblSales = await connPanda.query(qry);
        if(await tblSales[0].length>0){            
            for (let j = 0; j < tblSales[0].length; j++) {                
                const barcode_ =tblSales[0][j]['barcode'];
                const location_ =tblSales[0][j]['location'];
                const SoldQty_ =tblSales[0][j]['SoldQty'];
                const grn_qty_ =tblSales[0][j]['grn_qty'];                
                const qry_="update custom_sales_bck set tue_grn="+grn_qty_+", tue_sales="+SoldQty_+" where salestype='"+salestype+"' and location='"+location_+"' and barcode='"+barcode_+"' and sales_period='"+sales_period+"'";
                // console.log(qry_);
                await connWeb.query(qry_);  
            }
        }
        qry= await create_qry(currentWed,'',Array_barcode);
        tblSales = await connPanda.query(qry);console.log(currentWed);
        if(await tblSales[0].length>0){            
            for (let j = 0; j < tblSales[0].length; j++) {   
                const barcode_ =tblSales[0][j]['barcode'];
                const location_ =tblSales[0][j]['location'];
                const SoldQty_ =tblSales[0][j]['SoldQty'];
                const grn_qty_ =tblSales[0][j]['grn_qty'];                
                const qry_="update custom_sales_bck set wed_grn="+grn_qty_+", wed_sales="+SoldQty_+" where salestype='"+salestype+"' and location='"+location_+"' and barcode='"+barcode_+"' and sales_period='"+sales_period+"'";
                // console.log(qry_);
                await connWeb.query(qry_);  
            }
        }
        qry= await create_qry(currentThu,'',Array_barcode);
        tblSales = await connPanda.query(qry);console.log(currentThu);
        if(await tblSales[0].length>0){            
            for (let j = 0; j < tblSales[0].length; j++) {   
                const barcode_ =tblSales[0][j]['barcode'];
                const location_ =tblSales[0][j]['location'];
                const SoldQty_ =tblSales[0][j]['SoldQty'];
                const grn_qty_ =tblSales[0][j]['grn_qty'];                
                const qry_="update custom_sales_bck set thu_grn="+grn_qty_+", thu_sales="+SoldQty_+" where salestype='"+salestype+"' and location='"+location_+"' and barcode='"+barcode_+"' and sales_period='"+sales_period+"'";
                // console.log(qry_);
                await connWeb.query(qry_);                
            }
        }
        qry= await create_qry(currentFri,'',Array_barcode);
        tblSales = await connPanda.query(qry);console.log(currentFri);
        if(await tblSales[0].length>0){            
            for (let j = 0; j < tblSales[0].length; j++) {   
                const barcode_ =tblSales[0][j]['barcode'];
                const location_ =tblSales[0][j]['location'];
                const SoldQty_ =tblSales[0][j]['SoldQty'];
                const grn_qty_ =tblSales[0][j]['grn_qty'];                
                const qry_="update custom_sales_bck set fri_grn="+grn_qty_+", fri_sales="+SoldQty_+" where salestype='"+salestype+"' and location='"+location_+"' and barcode='"+barcode_+"' and sales_period='"+sales_period+"'";
                // console.log(qry_);
                await connWeb.query(qry_);  
            }
        }      
        qry= await create_qry(currentSat,'',Array_barcode);
        tblSales = await connPanda.query(qry);console.log(currentSat);
        if(await tblSales[0].length>0){            
            for (let j = 0; j < tblSales[0].length; j++) {   
                const barcode_ =tblSales[0][j]['barcode'];
                const location_ =tblSales[0][j]['location'];
                const SoldQty_ =tblSales[0][j]['SoldQty'];
                const grn_qty_ =tblSales[0][j]['grn_qty'];                
                const qry_="update custom_sales_bck set sat_grn="+grn_qty_+", sat_sales="+SoldQty_+" where salestype='"+salestype+"' and location='"+location_+"' and barcode='"+barcode_+"' and sales_period='"+sales_period+"'";
                // console.log(qry_);
                await connWeb.query(qry_);  
            }
        } 
        qry= await create_qry(currentSun,'',Array_barcode);
        tblSales = await connPanda.query(qry);console.log(currentSun);
        if(await tblSales[0].length>0){            
            for (let j = 0; j < tblSales[0].length; j++) {   
                const barcode_ =tblSales[0][j]['barcode'];
                const location_ =tblSales[0][j]['location'];
                const SoldQty_ =tblSales[0][j]['SoldQty'];
                const grn_qty_ =tblSales[0][j]['grn_qty'];                
                const qry_="update custom_sales_bck set sun_grn="+grn_qty_+", sun_sales="+SoldQty_+" where salestype='"+salestype+"' and location='"+location_+"' and barcode='"+barcode_+"' and sales_period='"+sales_period+"'";
                // console.log(qry_);
                await connWeb.query(qry_);  
            }
        } 
        qry= await create_qry(currentMon,'',Array_barcode);
        tblSales = await connPanda.query(qry);console.log(currentMon);
        if(await tblSales[0].length>0){            
            for (let j = 0; j < tblSales[0].length; j++) {   
                const barcode_  =tblSales[0][j]['barcode'];
                const location_ =tblSales[0][j]['location'];
                const SoldQty_  =tblSales[0][j]['SoldQty'];
                const grn_qty_  =tblSales[0][j]['grn_qty'];                
                const qry_="update custom_sales_bck set mon_grn="+grn_qty_+", mon_sale="+SoldQty_+" where salestype='"+salestype+"' and location='"+location_+"' and barcode='"+barcode_+"' and sales_period='"+sales_period+"'";
                // console.log(qry_);
                await connWeb.query(qry_);  
            }
        }
        await connWeb.commit();         
    } catch (error) {        
        if (connPanda) {connPanda.rollback();}
        if (connWeb) {connWeb.rollback();}
        console.log(error)
        process.exit();
    } finally {
      if (connPanda) {connPanda.release();}
      if (connWeb) {connWeb.release();}
    }    
    console.log('Populating '+salestype+' Done');     
}
console.clear();
async function process_sales(){
    const sales_period_="2024-08-27 to 2024-09-02";
    const tmpstamp = new Date();console.log('Populating process_data : '+tmpstamp);    
    const Array_Vege="'2051920','2051917','2051626','2051627','2051921','2051912','2051918','2051105','2050214','2050220','2050323','2050213','2050212','2050211','2051216','2051219','2050210','2051913','2051927'";
    // const Array_Vege="'2050211'";
    
    console.log('Vege - Processing');  
    await process_data('Vege',Array_Vege,sales_period_);  
    console.log('Sales - Vege - Done');        

    const Array_Fruits="'410822000020','410811000012','410812000010','2087221','2050229','2050222','2051601','2050706','2051215','2051620','2052205','2052206','2052217','2052221','2050801','2051617','2052507','2051348','2051218','2051217','2051901','2074971','2050402','2050117','2051341','2050410','2051224','2051226','2050374','2095532','410816000010','2051902','410703000020','2051106','2051631','2000613','2000614','2051004','2051019','2051303','2051948','2051312','2050227','2050217','2000118','2051656'";
    console.log('FRUITS - Processing');  
    await process_data('FRUITS',Array_Fruits,sales_period_);  
    console.log('Sales - FRUITS - Done');

    const Array_VegeKundasan="'2051905','2051621','2051625','2091978','2051937','2051936','2051603','2051103','2051915','2032304','2052001','2051919','2051934','2050344','2051206','2051903','2050325','2050329','2051504','2051502','2051201','2050301','2051814','2051924','2050341','2076797','2001039','2001122','2052004','2052005','2052015','2050326','2052601','2051381','2001041','2081126','2051210','2050204','2050209','2051930','2030601','2030602','2051300','2051346','2050370','2022940','2001038','2051403','2050116','2051602','1039564'";
    console.log('VegeKundasan - Processing');  
    await process_data('VegeKundasan',Array_VegeKundasan,sales_period_);  
    console.log('Sales - VegeKundasan - Done');

    process.exit();
}
process_sales();