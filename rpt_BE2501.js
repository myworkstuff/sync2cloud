const mysql2 = require('mysql2/promise');
require("dotenv").config();
var bodyParser =require('body-parser');
const moment = require('moment');
const { CONSTRAINTS } = require('cron/dist/constants');

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

async function process_data(salestype,Array_barcode){
    const tmpstamp = new Date();console.log('Populating process_data : '+tmpstamp);
    let connPanda,connWeb,qry='';    
    try {                
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();
        data=[];
        connWeb.query("DELETE FROM custom_sales_BE2501"); 
       
        for (let i = 0; i < Array_barcode.length; i++) {            
            const _barcode=Array_barcode[i];
            let iqry2 =" SELECT  description FROM backend.itemmaster a INNER JOIN itembarcode b ON a.itemcode=b.itemcode WHERE barcode='"+_barcode+"'"                         
            const tblPO = await connPanda.query(iqry2); 
            let _description=tblPO[0][0]['description']; 
            _description = _description.replace(/\\/g, " ");_description = _description.replace(/'/g, "\\'");                 
            qry="SELECT `code` FROM location WHERE LEFT(`code`,1)<>'f' AND LEFT(`code`,2)<>'DC' AND RIGHT(`code`,2)<>'DC' AND `code`<>'HQ'";
            const tbl_location = await connPanda.query(qry); 
            if(await tbl_location[0].length>0){
                for (let j = 0; j < tbl_location[0].length; j++) {      
                    const _location= tbl_location[0][j]['code'];
                    let iqry =" SELECT location,barcode,b.description,podate,b.refno,b.qty";    
                    iqry +=" FROM pomain a"
                    iqry +=" INNER JOIN pochild b ON a.refno=b.refno"
                    iqry +=" WHERE barcode IN('"+_barcode+"')"
                    iqry +=" AND location='"+_location+"'"
                    iqry +=" ORDER BY podate DESC limit 1"                    
                    const tblPO = await connPanda.query(iqry); 
                    let _qtyin=0,_refno="",_qoh=0;
                    if(await tblPO[0].length>0){_qtyin=tblPO[0][0]['qty'];_refno=tblPO[0][0]['refno'];}
                    iqry="SELECT FORMAT(qoh,0)qoh FROM itembarcode a,itemmaster_branch_stock b WHERE a.itemcode=b.itemcode AND a.barcode='"+_barcode+"' AND branch='"+_location+"'";
                    const tblqoh = await connPanda.query(iqry);
                    if(await tblqoh[0].length>0){_qoh=tblqoh[0][0]['qoh'];}else{_qoh=0;}if(_qoh<0){_qoh=0;}
                    iqry =" SELECT a.refno,b.qty";    
                    iqry +=" FROM dbnotemain a"
                    iqry +=" INNER JOIN dbnotechild b ON a.refno=b.refno"
                    iqry +=" WHERE barcode IN('"+_barcode+"')"
                    iqry +=" AND a.location='"+_location+"'"
                    iqry +=" ORDER BY docdate DESC limit 1"     
                    let _qtyout=0,_dnrefno=""               
                    const tblDN = await connPanda.query(iqry); 
                    if(await tblDN[0].length>0){_qtyout=tblDN[0][0]['qty'];_dnrefno=tblDN[0][0]['refno'];}
                    propose=(_qtyin-_qtyout-_qoh)+((_qtyin-_qtyout-_qoh)*10/100);                    
                    qry ="insert into custom_sales_BE2501(location,barcode,description,porefo,qtyin,dnrefno,qtyout,propose,qoh)value"
                    qry +="('"+_location+"','"+_barcode+"','"+_description+"','"+_refno+"',"+_qtyin+",'"+_dnrefno+"',"+_qtyout+","+propose+","+_qoh+")";
                    // console.log(qry);process.exit();
                    await connWeb.query(qry);                   
                }
            }   
        }
        qry="SELECT `code` FROM location WHERE LEFT(`code`,1)<>'f'";
        const tbl_location = await connPanda.query(qry);             
        for (let j = 0; j < tbl_location[0].length; j++) {  
            const _location= tbl_location[0][j]['code'];
            for (let i = 0; i < Array_barcode.length; i++) { 
                const _barcode=Array_barcode[i];
                let seriesdate='';let categories=[];let series=[];
                for (let x = 7; x > 0; x--) {
                    const date = new Date();
                    date.setDate(date.getDate() - x);
                    const formattedDate = date.toISOString().slice(0, 10);
                    // console.log(formattedDate);process.exit();
                    const rslt = await connPanda.query("SELECT SUM(qty)qty FROM frontend.poschild WHERE barcode ='"+_barcode+"' AND bizdate ='"+formattedDate+"' AND location='"+_location+"'");                        
                    let QTY=0;
                    if(await rslt[0].length>0){                            
                        QTY=rslt[0][0]['qty'] ?? 0;
                    }
                    series.push(QTY);
                    categories.push(formattedDate);
                }                
                seriesdate={'name': _barcode,'data':series};                
                qry="UPDATE custom_sales_BE2501 set series ='"+JSON.stringify({'name': _barcode,'data':series})+"',categories='"+JSON.stringify(categories)+"' where barcode='"+_barcode+"' and location='"+_location+"'";
                await connWeb.query(qry);                                  
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
    process.exit();
}
const item_Arr=['1041107','1041108','1041109','1011569','1011574','1023157','1023158','1029802','1029803','1029804','1037109','1038701','1038851','1038852','1038853','1039106','1039277','1039286','1040022','1040023','1040148','1040230','1040382','1040687'];
process_data('BE25-01',item_Arr);