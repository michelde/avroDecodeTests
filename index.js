const avro = require('avsc');
const fs = require("fs");
const avroSchemaParsed = JSON.parse(fs.readFileSync("./avroSchema.json", 'utf8'));
const avroMessageParsed = JSON.parse(fs.readFileSync("./avroMessage.json", 'utf8'));
const EventAvroType = avro.parse(avroSchemaParsed);

// encode message
const encodeMessage = data => {
  let messageEncoded = '';
  return new Promise((resolve, reject) => {

    const avroEncoder = new avro.streams.BlockEncoder(EventAvroType);
    avroEncoder.on('data', (chunk => {
      messageEncoded += chunk.toString('hex');
    }))
    avroEncoder.on('error', (err) => {
      reject(err)
    })
    avroEncoder.write(data); // We're writing the event directly instead of a buffer.
    avroEncoder.end();
    avroEncoder.on('end', (() => {
      resolve(messageEncoded)
    }))
  })
}

const decodeMessage = buffer => {
  return new Promise((resolve, reject) => {
    let result;
    const blockDecoder2 = new avro.streams.BlockDecoder(); // The streaming decoder.
    blockDecoder2.on("data", val => {
      result = val;
    });
    blockDecoder2.on("end", () => {
      resolve(result)
    });
    blockDecoder2.on("metadata", data => {
    })
    blockDecoder2.on("error", err => {
      reject(err)
    });
    blockDecoder2.end(buffer);
  })
};

async function startSingleTest() {
  const encodedMessageHex = await encodeMessage(avroMessageParsed);
  const message = await decodeMessage(Buffer.from(encodedMessageHex.toString(), "hex"));
  // console.log(`Decoded message: ${message}`);
}

async function performanceTest(counter) {
  const encodedMessageHex = await encodeMessage(avroMessageParsed);
  const encodedMessageBuffer = Buffer.from(encodedMessageHex.toString(), "hex")
  const startTime = new Date().getTime();
  for (let i = 0; i < counter; i+=1) {
    await decodeMessage(encodedMessageBuffer)
    if (i % 1000 === 0) {
      const used = process.memoryUsage().heapUsed / 1024 / 1024;
      console.log(`The script uses approximately ${Math.round(used * 100) / 100} MB at ${i} messages`);
    }
  }
  console.log(`Processing time for ${counter} Records ${new Date().getTime() - startTime}`);
}

startSingleTest();
performanceTest(5000);
