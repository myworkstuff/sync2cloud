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

async function process_data(salestype,inv_from,inv_to,Array_barcode){
    const tmpstamp = new Date();console.log('Populating process_data : '+tmpstamp);
    let connPanda,connWeb,qry=''    
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();
        for (let i = 0; i < Array_barcode.length; i++) {      
            const _barcode=Array_barcode[i];
            let __description = await connPanda.query("SELECT a.description,cast(DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + "+inv_from+")DAY)as char) invFrm,cast(DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + "+inv_to+") DAY) as char) invTo FROM itemmaster a INNER JOIN itembarcode b ON a.itemcode=b.itemcode WHERE barcode='"+_barcode+"'"); 
            let _description=__description[0][0]['description'];
            _description = _description.replace(/\\/g, " ");_description = _description.replace(/'/g, "\\'"); 
            const invFrm=__description[0][0]['invFrm'];
            const invTo=__description[0][0]['invTo'];
            qry="SELECT `code` FROM location WHERE LEFT(`code`,1)<>'f'";
            const tbl_location = await connWeb.query(qry); 
            if(await tbl_location[0].length>0){
                for (let j = 0; j < tbl_location[0].length; j++) {      
                    const _location= tbl_location[0][j]['code'];   
                    let _qoh  = await connPanda.query("SELECT qoh FROM backend.itemmaster_branch_stock a LEFT JOIN itembarcode b ON b.itemcode=a.itemcode WHERE barcode ='"+_barcode+"' AND branch='"+_location+"'");
                    if(_qoh[0].length==0){
                        _qoh=0;
                    }else{
                        _qoh=_qoh[0][0]['qoh'];
                    }                
                    let querry=" SELECT a.location,barcode,b.description,";
                    querry+=" IF(SoldbyWeight=1,TRUNCATE(SUM(weightvalue),2),TRUNCATE(qty,2)) SoldWeight,";
                    querry+=" IF(SoldbyWeight=1,TRUNCATE(SUM(weightPrice),2),price) weightPrice,";
                    querry+=" IF(SoldbyWeight=1,TRUNCATE((SUM(weightvalue)*SUM(weightPrice)),2),TRUNCATE((qty*price),2)) SoldPrice";
                    querry+=" FROM frontend.posmain a";
                    querry+=" INNER JOIN frontend.poschild b ON a.refno=b.refno AND a.location=b.location";
                    querry+=" WHERE a.bizdate BETWEEN DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + "+inv_from+") DAY) AND DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + "+inv_to+") DAY)";
                    querry+=" AND barcode ='"+_barcode+"' and a.location='"+_location+"'";
                    querry+=" GROUP BY location,barcode";
                    const tbl_sales = await connPanda.query(querry); 
                    querry=" replace into custom_sales(sales_period,salestype,location,barcode,description,soldweight,weightprice,soldprice,qoh,proposeqty)"
                    querry+="value"                    
                    if(await tbl_sales[0].length>0){
                        const SoldWeight=tbl_sales[0][0]['SoldWeight']                        
                        const weightPrice=tbl_sales[0][0]['weightPrice']
                        const SoldPrice=tbl_sales[0][0]['SoldPrice']
                        let propose_qty = SoldWeight+((SoldWeight*20)/100)
                        if(getFirstDecimalPointValue(propose_qty)<=4){
                            propose_qty=Math.floor(propose_qty)                            
                        }else{
                            propose_qty=Math.ceil(propose_qty)
                        }                    
                        querry+=" ('From "+invFrm+" to "+invTo+"','"+salestype+"','"+_location+"','"+_barcode+"','"+_description+"',"+SoldWeight+","+weightPrice+","+SoldPrice+","+_qoh+","+propose_qty+")"
                    }else{
                        querry+=" ('From "+invFrm+" to "+invTo+"','"+salestype+"','"+_location+"','"+_barcode+"','"+_description+"',0,0,0,"+_qoh+",0)"
                    }
                    await connWeb.query(querry);
                }
            }else{
                console.log('Nothing to process')
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
    const Array_Fruits=['410822000020','410811000012','410812000010','2087221','2050229','2050222','2051601','2050706','2051215','2051620','2052205','2052206','2052217','2052221','2050801','2051617','2052507','2051348','2051218','2051217','2051901','2074971','2050402','2050117','2051341','2050410','2051224','2051226','2050334','2050374','2095532','410816000010','2051902','2000034','410703000020','2051106','2030319','2051631','2000613','2000614','2051004','2051019','2051303','2051948','2051312','2050227','2050217','2000118','2051656'];
    await process_data('fruits friday',7,5,Array_Fruits);//Monday to Wednesday
    console.log('fruits friday - done')
    await process_data('fruits wednesday',4,1,Array_Fruits);//Thursday to Sunday
    console.log('fruits wednesday - done')
    process.exit();
}
process_sales();