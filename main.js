const fs = require('fs');
const readline = require('readline');

function sortFiles(amountOfFiles) {
  for(let i = 0; i < amountOfFiles; i++) {
    const currentFile = `files/b/${i+1}.txt`;
    const data = fs.readFileSync(currentFile, 'utf8').split('\n');
    data.pop();
    const sortedData = data.map((number) => parseInt(number)).sort((a, b) => a - b);
    fs.writeFileSync(currentFile, sortedData.map((number) => number.toString()).join('\n'));
  }
}

const splitInputFile = async (filename, numberOfFiles) => {
  const inputStream = fs.createReadStream(filename);
  const lineReader = readline.createInterface({
    input: inputStream,
    crlfDelay: Infinity
  });
  const outputStreams = [];

  async function readNextLine() {
    const line = await lineReader[Symbol.asyncIterator]().next();
    return line.done ? null : parseInt(line.value);
  }

  for (let i = 0; i < numberOfFiles; i++) {
    outputStreams.push(fs.createWriteStream(`files/b/${i+1}.txt`, { encoding: 'utf-8', highWaterMark: 1024 * 1024 }));
  }

  let prevNumber = -1;
  let currentFile = 0;
  while (prevNumber) {
    const currentNumber = await readNextLine();
    currentFile = (currentNumber < prevNumber ? currentFile+1 : currentFile) % numberOfFiles;
    prevNumber = currentNumber;
    if(prevNumber) outputStreams[currentFile].write(`${prevNumber}\n`);
  }
  
  outputStreams.forEach((stream) => stream.end());
}

const merge = async (numberOfFiles, filesGroup) => {
  const targetFilesGroup = filesGroup == 'b' ? 'c' : 'b';
  const lineReaders = [];
  const outputStreams = [];
  const currentChunks = [];

  async function readNextLine(index) {
    const line = await lineReaders[index][Symbol.asyncIterator]().next();
    return line.done ? null : parseInt(line.value);
  }

  for (let i = 0; i < numberOfFiles; i++) {
    lineReaders.push(readline.createInterface({
      input: fs.createReadStream(`files/${filesGroup}/${i+1}.txt`),
      crlfDelay: Infinity
    }));
    outputStreams.push(fs.createWriteStream(`files/${targetFilesGroup}/${i+1}.txt`, { highWaterMark: 1024 * 1024 }));
    outputStreams[i].on('error', (error) => console.log(error));
    currentChunks.push(await readNextLine(i));
  }
  
  let prevChunk = null;
  let currentFile = 0;
  while (!currentChunks.every((chunk) => chunk == null)) {
    const minIndex = currentChunks.indexOf(Math.min(...currentChunks.filter((chunk) => chunk !== null && chunk >= prevChunk)));
    if (minIndex == -1) {
      currentFile = (currentFile + 1) % numberOfFiles;
      prevChunk = null;
    } else {
      prevChunk = currentChunks[minIndex];
      outputStreams[currentFile].write(`${prevChunk}\n`);
      currentChunks[minIndex] = await readNextLine(minIndex);
    }
  }
  outputStreams.forEach((stream) => stream.end());
  return currentFile;
}

const balancedMultiwaySort = async (inputFile, isModified) => {
  await splitInputFile(`files/${inputFile}`, 50);
  if(isModified) sortFiles(50);
  let filledFiles = 1;
  let filesToMerge = 'b';
  while (filledFiles != 0) {
    filledFiles = await merge(50, filesToMerge)
    filesToMerge = filesToMerge == 'b' ? 'c' : 'b';
  }
}

const startTime = Date.now();
balancedMultiwaySort(process.argv[2], process.argv[3] == "true").then(() => {
  const time = new Date(Date.now() - startTime);
  console.log(`${time.getUTCHours()}:${time.getUTCMinutes()}:${time.getUTCSeconds()}`);
});