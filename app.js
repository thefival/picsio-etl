const AWS = require('aws-sdk')
const fs = require('fs')
const mysql = require('mysql2/promise')
const throttledQueue = require('throttled-queue')
const https = require('https')
const fetch = require('node-fetch')
const s3 = new AWS.S3
const os = require('os')
const path = require('path')
const TEMP_DIR = os.tmpdir()

const apiKey = '#';

process.env.AWS_ACCESS_KEY_ID = '#'
process.env.AWS_SECRET_ACCESS_KEY = '#'

const pool = mysql.createPool({
  connectionLimit: 16,
  host: 'localhost',
  user: 'root',
  password: '#',
  database: 'absdropbox',
})

const throttledRequestsPerSecond = 15

const throttle = throttledQueue(throttledRequestsPerSecond, 1000)

async function getSKUs() {
  const [rows] = await pool.query('SELECT primary_id FROM pimboe')
  return rows.map((row) => row.primary_id)
}

let v = 0
async function queryPicsio(text) {
  // console.log("3", text)
  console.log(`SKU COUNT ${v++}`)
  return new Promise(async (resolve, reject) => {
    const options = {
      hostname: 'api.pics.io',
      path: '/images/search',
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
    }

    const body = []
    const req = https.request(options, res => {
      res.on('data', d => {
        body.push(d)
      });
      res.on('end', () => {
        try {
          resolve(JSON.parse(body.toString()))
        } catch (error) {
          console.log("error", error)
          resolve([]); // Resolve with null to indicate error
        }
      })
    })
    req.on('error', reject)
    req.write(JSON.stringify({
      from: 0,
      size: 50,
      text,
      searchIn: ['name']
    }))
    req.end()
  })
}

let i = 0
let p = 0
let u = 0

async function processSKU(sku) {
  // console.log("2")
  const query = sku.toString()
  const matches = await queryPicsio(query)

  if (matches.images == undefined || matches == []) {
    console.log(`${sku} CORRUPTED, COUNT: ${i++}`)
    return
  }

  // console.log("4")

  for (const match of matches.images) {
    const assetId = match._id
    const assetName = match.name

    // console.log(assetId, assetName)
    await processFile(assetId, assetName, sku) // Wait for the download to finish before moving to the next file
  }
}

async function processFile(assetId, assetName, sku) {
  try {
    const tempFilePath = path.join(TEMP_DIR, assetName);
    const allowedExtensions = ['.jpg'];

    let extension = path.extname(assetName);
    if (!allowedExtensions.includes(extension)) {
      console.log(`File extension not allowed: ${assetName} COUNT: ${u++}`)
      return
    }

    let url = `https://api.pics.io/images/buildDownloadLink/${assetId}`
    const link = await fetch(url, {
      headers: {
        accept: 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
    })
    const downloadLink = await link.json()

    const downloadUrl = new URL(downloadLink)
    const headers = {};

    const tokenInUrl = downloadUrl.searchParams.get('t')
    if (tokenInUrl) {
      downloadUrl.searchParams.delete('t');
      headers.Authorization = `Bearer ${tokenInUrl}`
    }

    const fileResponse = await fetch(downloadUrl, { headers })
    const buffer = await fileResponse.buffer()

    fs.writeFileSync(tempFilePath, buffer)

    const fileContent = fs.readFileSync(tempFilePath)
    const folderName = sku; // Use assetId as the folder name

    const folderParams = {
      Bucket: 'jpg-picsio',
      Key: folderName + '/', // Append '/' to the folder name to indicate it's a folder
    };

    const folderExists = await s3.headObject(folderParams).promise()
      .then(() => true)
      .catch(() => false)

    if (!folderExists) {
      try {
        await s3.putObject(folderParams).promise(); // Create the folder in S3
        console.log(`Folder created in S3: ${folderName}`)
      } catch (error) {
        console.error(`Error creating folder in S3: ${error}`)
        return;
      }
    } else {
      console.log(`Folder already exists in S3: ${folderName}`)
    }

    const fileParams = {
      Bucket: 'jpg-picsio',
      Key: folderName + '/' + assetName, // Upload the file inside the folder
      Body: fileContent,
    };

    try {
      await s3.upload(fileParams).promise();
      console.log(`File uploaded to S3: ${assetName} COUNT: ${p++}`)
      fs.unlinkSync(tempFilePath); // Delete the temporary file
    } catch (error) {
      console.error(`Error uploading file to S3: ${error}`)
      return;
    }
  } catch (error) {
    console.error(`An error occurred: ${error} COUNT ${i++}`)
    // Handle the error here or return if necessary
  }
}

async function run() {
  const skus = await getSKUs()
  for (const sku of skus) {
    // console.log("1")
    await throttle(() => processSKU(sku))
    // console.log("5")
  }
}

run().catch(console.error)

