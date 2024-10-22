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
async function process_data(bizdates,Outelt){
    const tmpstamp = new Date();console.log('Populating sales : '+bizdates);
    let datas = [];
    let itemcode="'210497','210490','1008170','1008171','1008172','110285','110292','1037858','1037850','1037846','1037853','1037859','1037862','124544','124572','124530','979846','979839','979832','979853','450044','132356','134008','1015164','167531','12816','193641','342629','168245','222390','1028722','167643','167328','313530','14819','15988','14770','1040805','13251','1042032','426195'";
    let connPanda,connWeb,qry=''
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();
        await connWeb.query("DELETE FROM cksgroup_intra.tbl_sales where "+bizdates+"");
        qry="SELECT `code` FROM location WHERE `code`='"+Outelt+"'";
        const tbl_location = await connWeb.query(qry); 
        for (let j = 0; j < tbl_location[0].length; j++) {
            const _location= tbl_location[0][j]['code'];
            
            let query = "";
            query += "SELECT '"+bizdates+"' bizdate,a.location, ";
            query += "group_code div_id,group_desc div_disc, ";
            query += "a.dept dept_id, ";
            query += "ac.description deptdesc, ";
            query += "a.subdept,a.subdeptdesc,a.category,ad.description cat_desc, ";
            query += "ROUND(SUM(SalesAmt_by_lastcost),2) pos_cost, ";
            query += "CASE WHEN a.location LIKE '%DC%' THEN FORMAT(0,2) ELSE COALESCE(SUM(SalesPOS),0) END post_amount, ";
            query += "SUM(COALESCE(sales_si_qty,0))si_cost,SUM(COALESCE(sales_si_amt,0))si_amount, ";
            query += "COALESCE(cns.cn_amount,0)cn_cost,COALESCE(cns.cn_Qty,0)cn_amount, ";
            query += "SUM(COALESCE(ict_cost,0))ict_cost,SUM(COALESCE(ict_amount,0))ict_amount, ";
            query += "COALESCE(known_shrinkage,0)known_shrinkage, ";
            query += "COALESCE(UnKnown_Shrinkageamt,0)unknown_shrinkage ";
            query += "FROM locationstock_period a ";
            query += "INNER JOIN itemmaster aa ON a.itemcode=aa.itemcode ";
            query += "INNER JOIN ( ";
            query += "   SELECT * FROM view_subdept_div GROUP BY dept ";
            query += ") ab ON ab.dept=a.dept ";
            query += "INNER JOIN department ac ON ac.code=a.dept ";
            query += "INNER JOIN category ad ON ad.code=a.category ";
            query += "LEFT JOIN( ";
            query += "   SELECT location,b.consign,category, ";
            query += "   ROUND(SUM(totalprice),2) cn_amount,ROUND(SUM(Qty),2) cn_Qty ";
            query += "   FROM backend.cnnotemain a ";
            query += "   INNER JOIN backend.cnnotechild b ON a.refno=b.refno ";
            query += "   WHERE LEFT(docdate,7)='"+bizdates+"' ";
            query += "   AND LEFT(location,1)<>'f' AND ibt='2' AND SCtype='C' ";
            query += "   AND location='"+_location+"' ";
            query += "   GROUP BY location,category ";
            query += ")cns ON a.category=cns.category AND a.location=cns.location ";
            query += "LEFT JOIN( ";
            query += "   SELECT location,category,consign, ";
            query += "   ROUND((MAX(ins)-MAX(outs))) known_shrinkage, ";
            query += "   ROUND((MAX(qtyins)-MAX(qtyouts)),2) known_shrinkageqty ";
            query += "   FROM( ";
            query += "      SELECT location,code_group,adjtype,`type`,dept,subdept,category,consign, ";
            query += "      ROUND(CASE WHEN adjtype='in' THEN SUM(totalprice) ELSE 0 END+ 0.000001,0) ins, ";
            query += "      ROUND(CASE WHEN adjtype='out' THEN SUM(totalprice) ELSE 0 END+ 0.000001,0) outs, ";
            query += "      CASE WHEN adjtype='in' THEN SUM(qty) ELSE 0 END qtyins, ";
            query += "      CASE WHEN adjtype='out' THEN SUM(qty) ELSE 0 END qtyouts ";
            query += "      FROM backend.adjustmain a ";
            query += "      INNER JOIN backend.adjustchild b ON a.refno=b.refno ";
            query += "      INNER JOIN backend.set_master_code c ON a.reason=c.code_desc ";
            query += "      WHERE LEFT(docdate,7)='"+bizdates+"' ";
            query += "      AND trans_type='ADJUST_REASON' ";
            query += "      AND code_group IN('DISPOSAL','OWN USE') ";
            query += "      AND location='"+_location+"' ";
            query += "      GROUP BY code_group,location,`type`,adjtype,consign,category ";
            query += "   )s ";
            query += "   GROUP BY category,location ";
            query += ")shrnk ON a.category=shrnk.category AND a.location=shrnk.location ";
            query += "LEFT JOIN( ";
            query += "   SELECT location,category,consign, ";
            query += "   ROUND((MAX(ins)-MAX(outs))) UnKnown_Shrinkageamt, ";
            query += "   ROUND((MAX(qtyins)-MAX(qtyouts)),2) UnKnown_Shrinkageqty ";
            query += "   FROM( ";
            query += "      SELECT location,code_group,adjtype,`type`,dept,subdept,category,consign, ";
            query += "      ROUND(CASE WHEN adjtype='in' THEN SUM(totalprice) ELSE 0 END+ 0.000001,0) ins, ";
            query += "      ROUND(CASE WHEN adjtype='out' THEN SUM(totalprice) ELSE 0 END+ 0.000001,0) outs, ";
            query += "      CASE WHEN adjtype='in' THEN ROUND(SUM(qty),2)ELSE 0 END qtyins, ";
            query += "      CASE WHEN adjtype='out' THEN ROUND(SUM(qty),2)ELSE 0 END qtyouts ";
            query += "      FROM backend.adjustmain a ";
            query += "      INNER JOIN backend.adjustchild b ON a.refno=b.refno ";
            query += "      INNER JOIN backend.set_master_code c ON a.reason=c.code_desc ";
            query += "      WHERE LEFT(docdate,7)='"+bizdates+"' ";
            query += "      AND trans_type='ADJUST_REASON' ";
            query += "      AND code_group NOT IN('DISPOSAL','OWN USE') ";
            query += "      AND location='"+_location+"' ";
            query += "      GROUP BY location,code_group,`type`,adjtype,consign,category ";
            query += "   )s ";
            query += "   GROUP BY category,location ";
            query += ")ushrnk ON a.category=ushrnk.category AND a.location=ushrnk.location ";
            query += "LEFT JOIN( ";
            query += "   SELECT location,dept,subdept,category,ROUND(SUM(lastcost*qty),2)ict_cost,ROUND(SUM(totalprice),2)ict_amount ";
            query += "   FROM backend.simain a ";
            query += "   INNER JOIN sichild b ON a.refno=b.refno ";
            query += "   WHERE LEFT(invoicedate,7)='"+bizdates+"' ";
            query += "   AND location='"+_location+"' ";
            query += "   AND ibt=2 ";
            query += "   GROUP BY location,category ";
            query += ")ict ON a.category=ict.category AND ict.location=a.location ";
            query += "WHERE periodcode ='"+bizdates+"' ";
            query += "AND a.location='"+_location+"' ";
            query += "GROUP BY a.location,a.category ";
            query += "LIMIT 999999";
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
    await process_data('2024-10');
    console.log('Done');
    process.exit();
}
process_sales();