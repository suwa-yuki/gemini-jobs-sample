'use strict'
const {BigQuery} = require('@google-cloud/bigquery')
const {VertexAI} = require('@google-cloud/vertexai')
const {Storage} = require("@google-cloud/storage")
const {CLOUD_RUN_TASK_INDEX = 0} = process.env

const projectId = process.env.PROJECT_ID
const bq = new BigQuery()
const gcs = new Storage()
const vertexAI = new VertexAI({project: projectId, location: 'asia-northeast1'});
const model = vertexAI.preview.getGenerativeModel({
    model: "gemini-pro-vision",
    generation_config: {
        "max_output_tokens": 2048,
        "temperature": 0.4,
        "top_p": 1,
        "top_k": 32
    }
})
const prompt = "あなたはECサイトのコンテンツ管理者です。添付した商品画像に相応しいタグを生成してください。カンマ区切りで出力してください。"

const main = async () => {
    const query = `SELECT uri FROM ${projectId}.products.tags LIMIT 1 OFFSET ${CLOUD_RUN_TASK_INDEX}`
    const data = await bq.query(query)
    const row = data[0][0]
    const uri = row.uri
    console.log(`execute : ${uri}`)
    const [bucket, object] = uri.replace('gs://', '').split('/')

    const blob = await gcs.bucket(bucket).file(object).download()
    const base64 = Buffer.from(blob[0]).toString('base64')

    const request = {
        "contents": [{
            "role": "user",
            "parts": [
              {
                "text": prompt
              },
              {
                "inlineData": {
                  "mimeType": "image/jpeg",
                  "data": base64
                }
              }
            ]
          }
        ]
    }

    const stream = await model.generateContentStream(request)
    const response = await stream.response
    const tags = JSON.stringify(response.candidates[0].content.parts[0].text)

    const insert = `UPDATE \`${projectId}.products.tags\` SET tags = ${tags} WHERE uri = "${uri}"`
    await bq.query(insert)
}
main().catch(err => {
    console.error(err)
    process.exit(1)
})