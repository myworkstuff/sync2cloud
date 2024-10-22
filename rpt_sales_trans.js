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
        qry="SELECT * FROM cksgroup_intra.tbl_sales_trans where bizdate='"+bizdates+"'";
        const tbl_location = await connWeb.query(qry); 
        for (let j = 0; j < tbl_location[0].length; j++) {
            const _location= tbl_location[0][j]['location'];
            const _bizdate= tbl_location[0][j]['bizdate'];;
            let query = "";
            query = " SELECT bizdate,location,group_code,dept,subdept,category,COUNT(refno) AS refno_count";
            query += " FROM (";
            query += " SELECT DISTINCT refno,group_code,location,a.dept,a.subdept,category,LEFT(bizdate, 7) AS bizdate";
            query += " FROM frontend.poschild a";
            query += " INNER JOIN view_subdept_div b ON a.dept=b.dept ";
            query += " WHERE LEFT(bizdate,7)='"+_bizdate+"' AND location='"+_location+"' GROUP BY refno";
            query += ")a GROUP BY location,dept, bizdate";            
            const tbl_div = await connPanda.query(query); 
            for (let j1 = 0; j1 < tbl_div[0].length; j1++) {
                const _refno_count= tbl_location[0][j1]['refno_count'];
                const _dept= tbl_location[0][j1]['dept'];
            }
            console.log(tbl_div[0]);
            process.exit();
            let [results1] = await connPanda.query(query);        
            const POSTransCat =results1.length;            
            // await connWeb.query("UPDATE cksgroup_intra.tbl_sales set pos_trans='"+POSTransCat+"' WHERE LEFT(bizdate,7)='"+_bizdate+"' AND location='"+_location+"' AND category='"+_category+"'");
            console.log('POSTransCat '+POSTransCat);
            query = " SELECT * FROM frontend.poschild WHERE LEFT(bizdate,7)='"+_bizdate+"' AND location='"+_location+"' AND subdept='"+_subdept+"' AND category='"+_category+"'  GROUP BY refno";
            [results1] = await connPanda.query(query);        
            const POSTrans_subdept =results1.length;    
            console.log('POSTrans_subdept '+POSTrans_subdept)        
            query = " SELECT * FROM frontend.poschild WHERE LEFT(bizdate,7)='"+_bizdate+"' AND location='"+_location+"' AND dept='"+_dept_id+"' AND category='"+_category+"'  GROUP BY refno";
            [results1] = await connPanda.query(query);        
            const POSTransdept_idt =results1.length;
            console.log('POSTransdept_idt '+POSTransdept_idt)



            process.exit()
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
console.clear();
async function process_sales(){        
    await process_transaction('2024-08');
    // await process_transaction('2024-06');
    console.log('Done');
    process.exit();
}
process_sales();