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
async function get_qrydept(concept,conceptqry,bizdates,dept){
    var qry=`
        SELECT '${concept}' concept,'${bizdates}' perioddate,dept,
        ROUND(COALESCE(ROUND(SUM(Sales)),0),2) 'sales',
        ROUND(COALESCE(ROUND(SUM((disposal_amt))),0),2) disposal,
        ROUND(COALESCE(ROUND(SUM((unknown_shrinkage))),0),2) adjustment,
        ROUND(COALESCE(ROUND(SUM(Sales))-ROUND(SUM(COGS))-(ROUND(SUM(disposal_amt))+ROUND(SUM(unknown_shrinkage))),0),2) 'gross_profit',
        ROUND(COALESCE(ROUND((SUM(balanceamt)/SUM(SalesAmt_by_lastcost))*30,2),0),2)'doh',
        ROUND(COALESCE(SUM(SalesAmt_by_lastcost),0),2)SalesAmt_by_lastcost,
        ROUND(COALESCE(SUM(balanceamt),0),2)balanceamt
        FROM(
        SELECT a.location,a.dept,
        SUM(COALESCE(OpeningAmt,0))OpeningAmt,
        ROUND(SUM(salespos+COALESCE(sales_si_amt,0)-
        (COALESCE(creditamt,0)-(COALESCE(dnamt_cus,0)+
        (COALESCE(cnamt_cus_ibt,0)+COALESCE(cn_estore_amt,0)-
        COALESCE(dnamt_cus_ibt,0)-COALESCE(dn_estore_amt,0))+COALESCE(cnamt_sup,0)))),2)'Sales',
        COALESCE(SUM(SalesAmt_by_lastcost),0)'SalesAmt_by_lastcost',
        COALESCE(SUM(balanceamt),0)'balanceamt',
        ROUND(SUM(grn_ibt_amt)-SUM(sales_ibt_amt)+(SUM(cnamt_cus_ibt)+SUM(cnamt_sup_ibt))-(SUM(dnamt_cus_ibt)+SUM(dnamt_sup_ibt)),2)'IBTStockTransfer',
        ROUND(SUM(receivedamt-grn_ibt_amt-grn_inkind_amt))'PurchaseGRN',
        ROUND(SUM(debitamt-(cnamt_sup+(dnamt_sup_ibt-cnamt_sup_ibt )+dnamt_cus)))'PurchaseReturn',
        ROUND(SUM(claim_amt+claim_amt_manual),2)'PromotionClaim',
        ROUND(SUM(markdownamt_dn))'PurchaseDN',
        ROUND(SUM(hamperinamt-hamperoutamt))'StockTransform',
        ROUND(SUM(balanceqty*sellingprice_gross))'BalStock',
        SUM(a.Consign)Consign,
        ROUND(SUM(a.Consign*costmarginvalue/100))'ConsingCost',
        Lastcost,SalesTempQty,SalesQty,
        ROUND(SUM(COALESCE(OpeningAmt,0)),2)+
        ROUND(SUM(receivedamt-grn_ibt_amt-grn_inkind_amt))-
        ROUND(SUM(debitamt-(cnamt_sup+(dnamt_sup_ibt-cnamt_sup_ibt )+dnamt_cus)))-
        ROUND(SUM(markdownamt_dn))-
        ROUND(SUM(claim_amt+claim_amt_manual),2)+
        ROUND(SUM(hamperinamt-hamperoutamt))+
        ROUND(SUM(grn_ibt_amt)-SUM(sales_ibt_amt)+(SUM(cnamt_cus_ibt)+SUM(cnamt_sup_ibt))-(SUM(dnamt_cus_ibt)+SUM(dnamt_sup_ibt)),2)-
        ROUND(SUM(balanceamt))+
        ROUND(SUM(a.Consign*costmarginvalue/100))
        'COGS',
        ROUND(SUM(COALESCE(disposal_amt,0)),2)disposal_amt,
        ROUND(SUM((COALESCE(AdjustInAmt,0)-COALESCE(AdjustOutAmt,0)-COALESCE(disposal_amt,0)))) unknown_shrinkage
        FROM locationstock_period a
        WHERE a.periodcode='${bizdates}'
        AND ${conceptqry}
        AND a.dept='${dept}'
        GROUP BY a.location
        )p
    `;
    return qry;
}
async function get_qry(concept,conceptqry,bizdates,subdept){
    var qry=`
        SELECT '${concept}' concept,'${bizdates}' perioddate,subdept,
        ROUND(COALESCE(ROUND(SUM(Sales)),0),2) 'sales',
        ROUND(COALESCE(ROUND(SUM((disposal_amt))),0),2) disposal,
        ROUND(COALESCE(ROUND(SUM((unknown_shrinkage))),0),2) adjustment,
        ROUND(COALESCE(ROUND(SUM(Sales))-ROUND(SUM(COGS))-(ROUND(SUM(disposal_amt))+ROUND(SUM(unknown_shrinkage))),0),2) 'gross_profit',
        ROUND(COALESCE(ROUND((SUM(balanceamt)/SUM(SalesAmt_by_lastcost))*30,2),0),2)'doh',
        ROUND(COALESCE(SUM(SalesAmt_by_lastcost),0),2)SalesAmt_by_lastcost,
        ROUND(COALESCE(SUM(balanceamt),0),2)balanceamt
        FROM(
        SELECT a.location,a.subdept,
        SUM(COALESCE(OpeningAmt,0))OpeningAmt,
        ROUND(SUM(salespos+COALESCE(sales_si_amt,0)-
        (COALESCE(creditamt,0)-(COALESCE(dnamt_cus,0)+
        (COALESCE(cnamt_cus_ibt,0)+COALESCE(cn_estore_amt,0)-
        COALESCE(dnamt_cus_ibt,0)-COALESCE(dn_estore_amt,0))+COALESCE(cnamt_sup,0)))),2)'Sales',
        COALESCE(SUM(SalesAmt_by_lastcost),0)'SalesAmt_by_lastcost',
        COALESCE(SUM(balanceamt),0)'balanceamt',
        ROUND(SUM(grn_ibt_amt)-SUM(sales_ibt_amt)+(SUM(cnamt_cus_ibt)+SUM(cnamt_sup_ibt))-(SUM(dnamt_cus_ibt)+SUM(dnamt_sup_ibt)),2)'IBTStockTransfer',
        ROUND(SUM(receivedamt-grn_ibt_amt-grn_inkind_amt))'PurchaseGRN',
        ROUND(SUM(debitamt-(cnamt_sup+(dnamt_sup_ibt-cnamt_sup_ibt )+dnamt_cus)))'PurchaseReturn',
        ROUND(SUM(claim_amt+claim_amt_manual),2)'PromotionClaim',
        ROUND(SUM(markdownamt_dn))'PurchaseDN',
        ROUND(SUM(hamperinamt-hamperoutamt))'StockTransform',
        ROUND(SUM(balanceqty*sellingprice_gross))'BalStock',
        SUM(a.Consign)Consign,
        ROUND(SUM(a.Consign*costmarginvalue/100))'ConsingCost',
        Lastcost,SalesTempQty,SalesQty,
        ROUND(SUM(COALESCE(OpeningAmt,0)),2)+
        ROUND(SUM(receivedamt-grn_ibt_amt-grn_inkind_amt))-
        ROUND(SUM(debitamt-(cnamt_sup+(dnamt_sup_ibt-cnamt_sup_ibt )+dnamt_cus)))-
        ROUND(SUM(markdownamt_dn))-
        ROUND(SUM(claim_amt+claim_amt_manual),2)+
        ROUND(SUM(hamperinamt-hamperoutamt))+
        ROUND(SUM(grn_ibt_amt)-SUM(sales_ibt_amt)+(SUM(cnamt_cus_ibt)+SUM(cnamt_sup_ibt))-(SUM(dnamt_cus_ibt)+SUM(dnamt_sup_ibt)),2)-
        ROUND(SUM(balanceamt))+
        ROUND(SUM(a.Consign*costmarginvalue/100))
        'COGS',
        ROUND(SUM(COALESCE(disposal_amt,0)),2)disposal_amt,
        ROUND(SUM((COALESCE(AdjustInAmt,0)-COALESCE(AdjustOutAmt,0)-COALESCE(disposal_amt,0)))) unknown_shrinkage
        FROM locationstock_period a
        WHERE a.periodcode='${bizdates}'
        AND ${conceptqry}
        AND a.subdept='${subdept}'
        GROUP BY a.location
        )p
    `;
    return qry;
}
async function process_dept(perioddate,concept,loc){    
    let connPanda,connWeb
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();        
        qry="SELECT `code` FROM department WHERE `code` IN('10','11','12','13','14','15','16','17','18')";        
        const tbl_subdept = await connPanda.query(qry);         
        if(await tbl_subdept[0].length>0){
            for (let j = 0; j < tbl_subdept[0].length; j++) { 
                const _dept=tbl_subdept[0][j]['code'];
                console.log('Populating : '+_dept)
                let qry = await get_qrydept(concept,loc,perioddate,_dept);
                await connPanda.query("SET collation_connection = 'latin1_swedish_ci'");
                const [tbl_sales] = await connPanda.query(qry);                  
                const insertPromises = tbl_sales.map(row => {
                    const columns = Object.keys(row).join(',');
                    const values = Object.values(row).map(value => `'${detectDateType(value)}'`).join(',');                    
                    return connWeb.query(`REPLACE INTO cksgroup_intra.tbl_ytp_dept (${columns}) VALUES (${values})`);
                  }); 
                  await Promise.all(insertPromises);                   
            }
              
        }             
        await connWeb.commit();  
    } catch (error) {                
        if (connWeb) {connWeb.rollback();}
        console.log(error)
    } finally {      
      if (connWeb) {connWeb.release();}
    }        
}
async function process_data(perioddate,concept,loc){    
    let connPanda,connWeb
    try {
        connPanda = await dbpool.getConnection();
        connWeb = await dbpool2.getConnection();
        await connWeb.beginTransaction();        
        qry="SELECT `code` FROM subdept WHERE `code` IN('180','181','182','183','184','185','186','187')";        
        const tbl_subdept = await connPanda.query(qry);         
        if(await tbl_subdept[0].length>0){
            for (let j = 0; j < tbl_subdept[0].length; j++) { 
                const _subdept=tbl_subdept[0][j]['code'];
                let qry = await get_qry(concept,loc,perioddate,_subdept);
                await connPanda.query("SET collation_connection = 'latin1_swedish_ci'");
                const [tbl_sales] = await connPanda.query(qry);  
                console.log('Populating : '+_subdept)
                const insertPromises = tbl_sales.map(row => {
                    const columns = Object.keys(row).join(',');
                    const values = Object.values(row).map(value => `'${detectDateType(value)}'`).join(',');                    
                    return connWeb.query(`REPLACE INTO cksgroup_intra.tbl_ytp_subdept (${columns}) VALUES (${values})`);
                  }); 
                  await Promise.all(insertPromises);                   
            }
              
        }             
        await connWeb.commit();  
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
    const concept='SH';
    const loc="LEFT(a.location,1)<>'f'";
    // await process_data('2024-10',concept,loc);
    // await process_dept('2024-10',concept,loc);    
    for (let month = 1; month <= 12; month++) {
        const periodcode = `2022-${month.toString().padStart(2, '0')}`;
        console.log('Processing - '+periodcode);
        await process_data(periodcode,concept,loc);
        console.log('Done - '+periodcode);
        console.log('Processing - '+periodcode);
        await process_dept(periodcode,concept,loc);
        console.log('Done - '+periodcode);        
    }    
    process.exit();
}
process_sales();