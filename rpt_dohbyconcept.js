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
function escapeSingleQuotes(str) {return str.replace(/'/g, "\\'");}
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
  async function process_getDOH(periodcode){
    console.log('Populating process_getDOH : '+periodcode);
    let sourceConnection;
    let destinationConnection;              
    try {
        sourceConnection = await dbpool.getConnection();
        destinationConnection = await dbpool2.getConnection();            
        await destinationConnection.beginTransaction();  
        let query = `
        SELECT location, periodcode, dept, q.description,
        ROUND(SUM(COALESCE(balanceamt,0)),2) balanceamt,
        ROUND(SUM(COALESCE(COGS,0)),2) POSCOGS,
        ROUND(SUM(COALESCE(TotalSales,0)),2) 'POSSales',
        ROUND(COALESCE(DOH,0),2) 'DOH'
        FROM(
            SELECT a.periodcode, a.location, a.dept,
            ROUND(SUM(COALESCE(OpeningAmt,0)),2) 'OpeningAmt',
            ROUND(COALESCE(PurchaseGRN,0),2) 'PurchaseGRN',
            ROUND(COALESCE(PurchaseReturn,0),2) 'PurchaseReturn',
            ROUND(SUM(COALESCE(balanceamt,0)),2) 'balanceamt',
            ROUND(SUM(COALESCE(SalesAmt_by_lastcost,0)),2) 'SaleCost',
            ROUND(SUM(COALESCE(SalesPOS,0)),2) SalesPOS,
            ROUND(SUM(COALESCE(SalesAmt,0)),2) SalesAmt,
            ROUND(SUM(COALESCE(sales_si_amt,0)),2) SalesInvoice,
            ROUND(SUM(grn_ibt_amt)-SUM(sales_ibt_amt)+(SUM(cnamt_cus_ibt)+SUM(cnamt_sup_ibt))-(SUM(dnamt_cus_ibt)+SUM(dnamt_sup_ibt)),2) 'IBTStockTransfer',
            ROUND(SUM(COALESCE(OpeningAmt,0)),2)+ROUND(COALESCE(PurchaseGRN,0),2)-ROUND(COALESCE(PurchaseReturn,0),2)+ROUND(SUM(grn_ibt_amt)-SUM(sales_ibt_amt)+(SUM(cnamt_cus_ibt)+SUM(cnamt_sup_ibt))-(SUM(dnamt_cus_ibt)+SUM(dnamt_sup_ibt)),2)-ROUND(SUM(COALESCE(balanceamt,0)),2) 'COGS',
            ROUND(SUM(COALESCE(SalesPOS,0)),2)+ROUND(SUM(COALESCE(sales_si_amt,0)),2)-ROUND(COALESCE(cn_amount,0),2) 'TotalSales',
            (ROUND(SUM(COALESCE(SalesPOS,0)),2)+ROUND(SUM(COALESCE(sales_si_amt,0)),2)-ROUND(COALESCE(cn_amount,0),2))-(ROUND(SUM(COALESCE(OpeningAmt,0)),2)+ROUND(COALESCE(PurchaseGRN,0),2)-ROUND(COALESCE(PurchaseReturn,0),2)+ROUND(SUM(grn_ibt_amt)-SUM(sales_ibt_amt)+(SUM(cnamt_cus_ibt)+SUM(cnamt_sup_ibt))-(SUM(dnamt_cus_ibt)+SUM(dnamt_sup_ibt)),2)-ROUND(SUM(COALESCE(balanceamt,0)),2))-(ROUND(SUM(COALESCE(disposal_amt,0)),2)+ROUND(COALESCE(unknow_shrinkage,0),2)) 'Profit',
            ROUND(COALESCE(cn_amount,0)) CN,
            ROUND(SUM(COALESCE(disposal_amt,0)),2) disposal_amt, COALESCE(unknow_shrinkage,0) unknow_shrinkage,
            (ROUND(SUM(COALESCE(SalesPOS,0)),2)+ROUND(SUM(COALESCE(sales_si_amt,0)),2)-ROUND(COALESCE(cn_amount,0),2)) /
            (ROUND(SUM(COALESCE(OpeningAmt,0)),2)+ROUND(COALESCE(PurchaseGRN,0),2)-ROUND(COALESCE(PurchaseReturn,0),2)+ROUND(SUM(grn_ibt_amt)-SUM(sales_ibt_amt)+(SUM(cnamt_cus_ibt)+SUM(cnamt_sup_ibt))-(SUM(dnamt_cus_ibt)+SUM(dnamt_sup_ibt)),2)-ROUND(SUM(COALESCE(balanceamt,0)),2)) * 30 'DOH'
            FROM locationstock_period a
            LEFT JOIN(
                SELECT LEFT(docdate,7) periodcode,
                location, b.consign, dept,
                ROUND(SUM(totalprice),2) cn_amount
                FROM backend.cnnotemain a
                INNER JOIN backend.cnnotechild b ON a.refno=b.refno
                WHERE ibt='2' AND SCtype='C' AND LEFT(docdate,7)='${periodcode}'
                GROUP BY LEFT(docdate,7), dept, location
            ) cns ON a.dept=cns.dept AND a.periodcode=cns.periodcode AND a.location=cns.location
            LEFT JOIN(
                SELECT location, dept, periodcode,
                ROUND((MAX(ins)-MAX(outs)),2) unknow_shrinkage,
                ROUND((MAX(qtyins)-MAX(qtyouts)),2) UnKnown_Shrinkageqty
                FROM(
                    SELECT LEFT(docdate,7) periodcode, location, code_group, adjtype, type, dept, subdept, category, consign,
                    ROUND(CASE WHEN adjtype='in' THEN SUM(totalprice) ELSE 0 END,2) ins,
                    ROUND(CASE WHEN adjtype='out' THEN SUM(totalprice) ELSE 0 END,2) outs,
                    CASE WHEN adjtype='in' THEN ROUND(SUM(qty),2) ELSE 0 END qtyins,
                    CASE WHEN adjtype='out' THEN ROUND(SUM(qty),2) ELSE 0 END qtyouts
                    FROM backend.adjustmain a
                    INNER JOIN backend.adjustchild b ON a.refno=b.refno
                    INNER JOIN backend.set_master_code c ON a.reason=c.code_desc
                    WHERE LEFT(docdate,7)='${periodcode}'
                    AND trans_type='ADJUST_REASON'
                    AND code_group NOT IN('DISPOSAL','OWN USE')
                    GROUP BY location, code_group, type, adjtype, consign, dept, LEFT(docdate,7)
                ) s
                GROUP BY dept, location, periodcode
            ) unkShrink ON a.dept=unkShrink.dept AND a.periodcode=unkShrink.periodcode AND a.location=unkShrink.location
            LEFT JOIN(
                SELECT location, LEFT(docdate,7) periodcode, b.dept, SUM(TotalPrice) PurchaseGRN
                FROM grmain a
                INNER JOIN grchild b ON a.refno=b.refno
                WHERE LEFT(docdate,7)='${periodcode}'
                AND ibt=0
                GROUP BY location, LEFT(docdate,7), dept
            ) purgrn ON a.dept=purgrn.dept AND a.periodcode=purgrn.periodcode AND a.location=purgrn.location
            LEFT JOIN(
                SELECT ibt, b.location, LEFT(docdate,7) periodcode, b.dept, SUM(TotalPrice) PurchaseReturn
                FROM dbnotemain a
                INNER JOIN dbnotechild b ON a.refno=b.refno AND a.location=b.location
                WHERE LEFT(docdate,7)='${periodcode}'
                AND ibt=0
                GROUP BY location, LEFT(docdate,7), dept
            ) purreturn ON a.dept=purreturn.dept AND a.periodcode=purreturn.periodcode AND a.location=purreturn.location
            WHERE a.periodcode='${periodcode}'
            GROUP BY a.location, a.dept
        ) p
        LEFT JOIN department q ON p.dept=q.code
        GROUP BY location, dept
        `;   
        await sourceConnection.query("SET collation_connection = 'latin1_swedish_ci'");         
        const [results] = await sourceConnection.query(query);    
        const insertPromises = results.map(row => {
          const columns = Object.keys(row).join(',');
          const values = Object.values(row).map(value => `'${detectDateType(value)}'`).join(',');
          return destinationConnection.query(`REPLACE INTO cksgroup_intra.charts_doh_dept (${columns}) VALUES (${values})`);
        });    
        await Promise.all(insertPromises);            
        await destinationConnection.commit();
        console.log('Populating '+periodcode+' Done');           
    } catch (error) {        
        if (sourceConnection) {sourceConnection.rollback();}
        if (destinationConnection) {destinationConnection.rollback();}
        console.log(error)
    } finally {
      if (sourceConnection) {sourceConnection.release();}
      if (destinationConnection) {destinationConnection.release();}
    }           
}  
async function process_data_dept(periodcode,concept,conceptlist){
    console.log('Populating process_data_dept : '+periodcode);
    let sourceConnection;
    let destinationConnection;              
    try {
        sourceConnection = await dbpool.getConnection();
        destinationConnection = await dbpool2.getConnection();            
        await destinationConnection.beginTransaction();  
        let query="SELECT '"+concept+"' location,periodcode,a.dept,b.description, ";
        query+="COALESCE(ROUND(SUM(balanceamt),2),0)balanceamt,";
        query+="COALESCE(ROUND(SUM(SalesAmt_by_lastcost),2),0)'POSCOGS',";
        query+="COALESCE(ROUND(SUM(salespos),2),0)'POSSales',";
        query+="COALESCE(ROUND((ROUND(SUM(balanceamt),2)/ROUND(SUM(SalesAmt_by_lastcost),2))*30,2),0) DOH ";
        query+="FROM locationstock_period a ";
        query+="INNER JOIN itemmaster aa ON a.itemcode=aa.itemcode ";
        query+="INNER JOIN department b ON aa.dept=b.code ";
        query+="WHERE periodcode ='"+periodcode+"' and location in("+conceptlist+") ";
        query+="GROUP BY periodcode,a.dept ";  
        query+="ORDER BY location,periodcode,a.dept";
        // console.log(query);
        // process.exit();
        const [results] = await sourceConnection.query(query);    
        const insertPromises = results.map(row => {
          const columns = Object.keys(row).join(',');
          const values = Object.values(row).map(value => `'${detectDateType(value)}'`).join(',');
          return destinationConnection.query(`REPLACE INTO cksgroup_intra.charts_doh_concept (${columns}) VALUES (${values})`);
        });    
        await Promise.all(insertPromises);            
        await destinationConnection.commit();
        console.log('Populating '+periodcode+' Done');           
    } catch (error) {        
        if (sourceConnection) {sourceConnection.rollback();}
        if (destinationConnection) {destinationConnection.rollback();}
        console.log(error)
    } finally {
      if (sourceConnection) {sourceConnection.release();}
      if (destinationConnection) {destinationConnection.release();}
    }           
}
async function process_sales(){
    // await process_getDOH('2024-09');
    const concept='johor';
    const SH="'TRN','MGT','MLN','DGO','SPG','PTS','PR2','KNG','BDS','M88','SJT','PPR','TLP','SBBK','SLDO'";
    const Grocer="'TRN','MGT','MLN','DGO','SPG','PTS','PR2','KNG','BDS','M88','SJT','PPR','TLP','SBBK','DCB'";
    const FH="'FABR','FBPT','FBPW','FBTR','FDC','FDSR','FGLP','FKPP','FKPS','FLBS','FMSG','FPGH','FPKR','FPLH','FPRG','FPRS','FSAB','FSGM','FSIK','FSLG','FSSL','FSTK','FSTP','FTKM','FTKP','FTMN','FTMS','FTMU','FTPT'";
    for (let month = 1; month <= 12; month++) {
        const periodcode = `2022-${month.toString().padStart(2, '0')}`;        
        await process_data_dept(periodcode,concept,FH);
    }
    process.exit();
}
process_sales();
