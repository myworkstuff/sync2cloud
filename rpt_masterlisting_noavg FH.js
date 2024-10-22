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

async function get_qry(concept,conceptqry,bizdates,subdept){
    var qry=`
        SELECT '${concept}' concept,'${bizdates}' perioddate,subdept,
    ROUND(SUM(TotalSales))'sales',
    ROUND(SUM(COALESCE(disposal_amt,0)))'disposal'
    FROM(
    SELECT a.subdept,
    ROUND(SUM(COALESCE(SalesPOS,0)),2)+ROUND(SUM(COALESCE(sales_si_amt,0)),2)-ROUND(COALESCE(cn_amount,0),2)'TotalSales',
    ROUND(SUM(COALESCE(disposal_amt,0)),2)disposal_amt
    FROM locationstock_period a
    LEFT JOIN itemmaster_branch_stock aa ON a.itemcode=aa.itemcode AND a.location=aa.branch
    LEFT JOIN(
        SELECT LEFT(docdate,7)periodcode,
        location,b.consign,subdept,
        ROUND(SUM(totalprice),2) cn_amount
        FROM backend.cnnotemain a 
        INNER JOIN backend.cnnotechild b ON a.refno=b.refno 	
        WHERE subdept='${subdept}' AND ibt='2' AND SCtype='C' 	
        GROUP BY LEFT(docdate,7),subdept,location
    )cns ON a.subdept=cns.subdept AND a.periodcode=cns.periodcode AND a.location=cns.location
    LEFT JOIN(
    SELECT location,subdept,periodcode, 
    ROUND((MAX(ins)-MAX(outs)),2) unknown_shrinkage, 
    ROUND((MAX(qtyins)-MAX(qtyouts)),2) UnKnown_Shrinkageqty 
        FROM( 
            SELECT LEFT(docdate,7) periodcode,location,code_group,adjtype,type,dept,subdept,category,consign, 
            ROUND(CASE WHEN adjtype='in' THEN SUM(totalprice) ELSE 0 END,2) ins, 
            ROUND(CASE WHEN adjtype='out' THEN SUM(totalprice) ELSE 0 END,2) outs, 
            CASE WHEN adjtype='in' THEN ROUND(SUM(qty),2)ELSE 0 END qtyins, 
            CASE WHEN adjtype='out' THEN ROUND(SUM(qty),2)ELSE 0 END qtyouts 
            FROM backend.adjustmain a 
            INNER JOIN backend.adjustchild b ON a.refno=b.refno 
            INNER JOIN backend.set_master_code c ON a.reason=c.code_desc 
            WHERE LEFT(docdate,7)='${bizdates}' 
            AND trans_type='ADJUST_REASON' 
            AND code_group NOT IN('DISPOSAL','OWN USE') 
            AND subdept='${subdept}' 
            GROUP BY location,code_group,type,adjtype,consign,subdept,LEFT(docdate,7) 
        )s 
    GROUP BY subdept,location,periodcode 
    )unkShrink ON a.subdept=unkShrink.subdept AND a.periodcode=unkShrink.periodcode AND a.location=unkShrink.location
    LEFT JOIN(
        SELECT location,LEFT(docdate,7) periodcode,b.subdept,SUM(TotalPrice)PurchaseGRN
        FROM grmain a
        INNER JOIN grchild b ON a.refno=b.refno
        WHERE LEFT(docdate,7)='${bizdates}'
        AND ibt=0 AND b.subdept='${subdept}'
        GROUP BY location,LEFT(docdate,7),subdept
    )purgrn ON a.subdept=purgrn.subdept AND a.periodcode=purgrn.periodcode AND a.location=purgrn.location
    LEFT JOIN(
        SELECT ibt,b.location,LEFT(docdate,7) periodcode,b.subdept,SUM(TotalPrice)PurchaseReturn
        FROM dbnotemain a
        INNER JOIN dbnotechild b ON a.refno=b.refno AND a.location =b.location 
        WHERE LEFT(docdate,7)='${bizdates}'
        AND ibt=0 AND b.subdept='${subdept}'
        GROUP BY location,LEFT(docdate,7),subdept
    )purreturn ON a.subdept=purreturn.subdept AND a.periodcode=purreturn.periodcode AND a.location=purreturn.location
    WHERE a.periodcode='${bizdates}'
    AND ${conceptqry} 
    AND a.subdept='${subdept}'
    GROUP BY a.location
    )p
    `;
    return qry;
}

async function process_data(concept,concept_){    
    let connPanda,connWeb
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        // await connWeb.beginTransaction();        
        qry=`
        SELECT '${concept}' concept,a.itemcode
        FROM itemmaster_listed_branch a
        INNER JOIN set_concept b ON a.branch=b.concept
        INNER JOIN set_concept_branch c ON b.concept_guid=c.concept_guid
        LEFT JOIN  itemmaster d ON a.itemcode=d.itemcode
        INNER JOIN department c1 ON d.dept=c1.code
        INNER JOIN subdept d1 ON d.subdept=d1.code
        INNER JOIN category e1 ON d.category=e1.code
        INNER JOIN brand ee ON d.brand=ee.code
        INNER JOIN set_group_dept f ON d.dept=f.DEPT_CODE
        INNER JOIN set_group g ON f.GROUP_CODE=g.GROUP_CODE
        WHERE LEFT(c.branch,1) ${concept_}
        
        GROUP BY a.itemcode 
        `;
        const tbl_itemlist = await connPanda.query(qry);  

        const [tbl_sales2] = await connPanda.query(qry);  
        const insertPromises = tbl_sales2.map(row => {
        const columns = Object.keys(row).join(',');
        const values = Object.values(row).map(value => `'${detectDateType(value)}'`).join(',');
            return connWeb.query(`insert INTO cksgroup_intra.tbl_masterlisting_noavg_FH (${columns}) VALUES (${values})`);
        }); 
        await Promise.all(insertPromises)

        const tCount=tbl_itemlist[0].length;
        if(await tbl_itemlist[0].length>0){
            for (let j = 0; j < tbl_itemlist[0].length; j++) { 
                const _itemcode=tbl_itemlist[0][j]['itemcode'];
                console.log(j+'/'+tCount)
                let qry=`
                SELECT '${concept}' concept,i.code 'SupCode',i.name 'SupName',
    ab.itemcode,ab.packsize,ab.um, abb.barcode, ab.description,
    ROUND(SUM(a.SoldWeight), 2) AS TotalSoldWeight,
    ROUND(SUM(a.SoldAmt), 2) AS TotalSoldAmt,
    ROUND(SUM(a.LastCost), 2) AS LastCost,
    COUNT(DISTINCT a.location) AS 'No_Involved_outlet',
    COALESCE(ROUND((ROUND(SUM(a.SoldAmt), 2)- ROUND(SUM(a.LastCost), 2)) / COUNT(DISTINCT a.location), 2),0) AS Profit_Outlet,
    ROUND((ROUND(SUM(a.SoldAmt), 2) - ROUND(SUM(a.LastCost), 2)), 2) AS Profit,
    consign AS 'IsConsignment',
    COALESCE(h.first_gr_date,'') '1stGRDate',COALESCE(h.QTY,0)1stGRQTY,
    COALESCE(hh.last_gr_date,'') 'LastGRDate',COALESCE(hh.QTY,0)LastGRQTY,
    CASE 
    WHEN DISABLE = 0 THEN 'Active'
    WHEN DISABLE = 1 THEN 'Disable'
    WHEN DISABLE = 2 THEN 'Delisted'
    END 'Status'
    FROM (
            SELECT barcode, itemcode, location, IF(SoldbyWeight = 1, SUM(weightvalue), SUM(qty)) AS SoldWeight, 
            ROUND(SUM(amount), 2) AS SoldAmt, ROUND(SUM(LastCost * qty),2) AS LastCost
            FROM frontend.poschild
            WHERE LEFT(bizdate, 7)>=LEFT(DATE(DATE_SUB(NOW(), INTERVAL 12 MONTH)),7)
            AND itemcode ='${_itemcode}'
            AND LEFT(location,1)  ${concept_}
            GROUP BY location, itemcode
    ) a
    LEFT JOIN itemmaster ab ON a.itemcode = ab.itemcode
    LEFT JOIN itembarcode abb ON ab.itemcode = abb.itemcode
    LEFT JOIN department c ON ab.dept = c.code
    LEFT JOIN subdept d ON ab.subdept = d.code
    LEFT JOIN category e ON ab.category = e.code
    LEFT JOIN brand ee ON ab.brand = ee.code
    LEFT JOIN set_group_dept f ON ab.dept = f.DEPT_CODE
    LEFT JOIN set_group g ON f.GROUP_CODE = g.GROUP_CODE
    LEFT JOIN (
            SELECT a.RefNo,itemcode, GRDate AS first_gr_date,Qty
            FROM grmain a
            INNER JOIN grchild b ON a.refno = b.refno
            WHERE LEFT(a.location,1)  ${concept_}
            AND b.itemcode ='${_itemcode}'
            AND LEFT(docdate,7)>=LEFT(DATE(DATE_SUB(NOW(), INTERVAL 12 MONTH)),7)
            ORDER BY GRDate ASC
            LIMIT 1
    ) h ON a.itemcode = h.itemcode
    LEFT JOIN (
            SELECT a.code,a.RefNo,itemcode, GRDate AS last_gr_date,Qty
            FROM grmain a
            INNER JOIN grchild b ON a.refno = b.refno
            WHERE LEFT(a.location,1)  ${concept_}
            AND b.itemcode ='${_itemcode}'
            AND LEFT(docdate,7)>=LEFT(DATE(DATE_SUB(NOW(), INTERVAL 12 MONTH)),7)
            ORDER BY GRDate DESC
    ) hh ON a.itemcode = hh.itemcode
    LEFT JOIN (SELECT itemcode,CODE,NAME FROM itemmastersupcode GROUP BY itemcode) i ON a.itemcode=i.itemcode
    GROUP BY a.itemcode`;                
    // console.log(qry);
    // process.exit();
            const [tbl_sales] = await connPanda.query(qry);  
            const insertPromises = tbl_sales.map(row => {
            const columns = Object.keys(row).join(',');
            const values = Object.values(row).map(value => `'${detectDateType(value)}'`).join(',');
                return connWeb.query(`replace INTO cksgroup_intra.tbl_masterlisting_noavg_FH (${columns}) VALUES (${values})`);
            }); 
            await Promise.all(insertPromises)                
            }
        }
        // await connWeb.commit();  
    } catch (error) {                
        if (connWeb) {connWeb.rollback();}
        console.log(error)
    } finally {      
      if (connWeb) {connWeb.release();}
    }    
       
}
console.clear();
async function process_sales(){
    console.clear();    
    await process_data('fh',"= 'f'");console.log('Populating FH');  
    process.exit();
}
process_sales();