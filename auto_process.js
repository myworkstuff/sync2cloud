const mysql2 = require('mysql2/promise');
require("dotenv").config();
var bodyParser =require('body-parser');
const moment = require('moment');
const { format, startOfYear, startOfMonth, endOfMonth, addMonths } = require('date-fns');

const CronJob = require('cron').CronJob;
const { exec } = require('child_process');
console.clear();
console.log('Auto Run started');
const jobs = new CronJob("0 8 * * *", async function() {
    exec('node rpt_hamper.js', (error, stdout, stderr) => {
        console.log('Running rpt_hamper.js');
        if (error) {console.error(`Error: ${error.message}`);return;}
        if (stderr) {console.error(`Stderr: ${stderr}`);return;}
        console.log(`rpt_hamper Output: ${stdout}`);
    });
    exec('node rpt_BE2501.js', (error, stdout, stderr) => {
        console.log('Running rpt_BE2501.js');
        if (error) {console.error(`Error: ${error.message}`);return;}
        if (stderr) {console.error(`Stderr: ${stderr}`);return;}
        console.log(`rpt_BE2501 Output: ${stdout}`);
    });
    exec('node rpt_promotion_insentive.js', (error, stdout, stderr) => {
        console.log('Running rpt_promotion_insentive.js');
        if (error) {console.error(`Error: ${error.message}`);return;}
        if (stderr) {console.error(`Stderr: ${stderr}`);return;}
        console.log(`rpt_promotion_insentive Output: ${stdout}`);
    });  
}, null, true, 'Asia/Kuala_Lumpur');

const jobs2 = new CronJob("0 1 * * *", async function() {
    exec('node rpt_dailysales.js', (error, stdout, stderr) => {
        console.log('Running rpt_dailysales.js');
        if (error) {console.error(`Error: ${error.message}`);return;}
        if (stderr) {console.error(`Stderr: ${stderr}`);return;}
        console.log(`rpt_dailysales Output: ${stdout}`);
    });       
    exec('node rpt_highshrinkage.js', (error, stdout, stderr) => {
        console.log('Running rpt_highshrinkage.js');
        if (error) {console.error(`Error: ${error.message}`);return;}
        if (stderr) {console.error(`Stderr: ${stderr}`);return;}
        console.log(`rpt_highshrinkage Output: ${stdout}`);
    });
    // exec('node rpt_sales.js', (error, stdout, stderr) => {
    //     console.log('Running rpt_sales.js');
    //     if (error) {console.error(`Error: ${error.message}`);return;}
    //     if (stderr) {console.error(`Stderr: ${stderr}`);return;}
    //     console.log(`rpt_sales Output: ${stdout}`);
    // });
    exec('node tpy_ytd_subdept.js', (error, stdout, stderr) => {
        console.log('Running tpy_ytd_subdept.js');
        if (error) {console.error(`Error: ${error.message}`);return;}
        if (stderr) {console.error(`Stderr: ${stderr}`);return;}
        console.log(`tpy_ytd_subdept Output: ${stdout}`);
    });    
}, null, true, 'Asia/Kuala_Lumpur');
jobs.start();
jobs2.start();