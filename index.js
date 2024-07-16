const axios = require('axios');
require('dotenv').config()
const knexLib = require('knex');

// Konfigurasi database MS SQL
const {
  DB_DIALECT,
  DB_USERNAME,
  DB_PASSWORD,
  DB_HOST,
  DB_NAME,
  DB_PORT,
  DB_SYNC,
} = process.env

// Konfigurasi Knex untuk database MS SQL
const knex = knexLib({
  client: 'mssql',
  connection: {
    user: DB_USERNAME,
    password: DB_PASSWORD,
    host: DB_HOST, // contoh: 'localhost'
    database: DB_NAME,
    options: {
      // encrypt: true, // Gunakan jika Anda menghubungkan ke Azure SQL Database atau membutuhkan enkripsi
      trustServerCertificate: true // Tambahkan opsi ini untuk menerima sertifikat self-signed
    }
  }
});

// Fungsi untuk hit API
async function hitApiSO(url, body) {
  try {
    const {
      TOKEN
    } = process.env

    let response = await axios.post(url, body, {
      headers: {
        'Authorization': TOKEN,
      }
    });

    return response.status;
  } catch (err) {
    console.error('API request error', err);
  }
}

// Contoh penggunaan
async function main() {
  // Koneksi ke database dan menjalankan query
  const query = `SELECT o.id, soj.body 
    FROM orders o
    LEFT JOIN sales_orders_jobs soj ON
    o.id = soj.order_id 
    WHERE 
    o.id = soj.order_id 
    AND soj.status = 2
    AND o.status_so = 0
    AND o.created_at BETWEEN '2024-07-12 00:00:00' AND '2024-07-17 00:00:00';`;

    const data_so = await knex.raw(query);


    const apiUrl = process.env.API_URL;
    const batchSize = 5; // Ukuran batch untuk menghit API
    const interval = 0;

    // Mengelompokkan data menjadi batch
    for (let i = 0; i < data_so.length; i += batchSize) {
      const batch = data_so.slice(i, i + batchSize);
  
      // Mengumpulkan semua promise untuk permintaan API dalam batch
      const apiPromises = batch.map(async (data) => {        
        console.log('trying hit API')
        const apiResponse = await hitApiSO(apiUrl, JSON.parse(data.body));

        if(apiResponse == 201) {
          console.log(apiResponse, `SUCCESS`)
          await knex('import.sales_orders_jobs_email').insert({order_id: data.id, status: 1});
        } else {
          console.log("error")
        }
      });
  
      // Menunggu semua permintaan API dalam batch selesai
      await Promise.all(apiPromises);
      
      // Tunggu interval sebelum memproses batch berikutnya
      if (i + batchSize < data_so.length) {
        await new Promise((resolve) => setTimeout(resolve, interval));
      }
    }
    return data_so.length
}

main().then((data) => {
  console.log(`Jumlah data: ${data}`)
  console.log("Selesai!")
}).finally(() => {
  // Pastikan untuk menghancurkan koneksi Knex setelah selesai
  knex.destroy();
});

