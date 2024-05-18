"use strict"


const EventEmitter = require('node:events')
/**
 * The `SourceReader` class is responsible for maintaining a Map of worker
 * Promises.  
 * 
 * Emits 3 events:
 * 
 *  `data`: Provides the next log that was fetched from a source
 * 
 *  `drained`: Indicates a source has been drained.
 * 
 *  `done`: All sources are done processing.
 * 
*/
class SourceReader extends EventEmitter {
  /**
   * `Map<number, Promise>`
   * 
   *  `number` - An id for the promise, so we can remove the element later.
   * 
   *  `Promise` - The promise that gets the next log from the source.
   */
  workers = new Map()

  /**
   * @param {Array} logSources
   *  The array of sources we're ingesting
   */
  constructor(logSources) {
    super()
    this.sources = logSources
    this.workers = new Map()
  }

  /**
   * Checks each source to see if they're fully drained.
   * 
   * @returns `true`, if all sources are drained, `false` otherwise.
   */
  allSourcesDrained() {
    return this.sources.reduce((acc, source) => acc && source.drained, true)
  }

  /**
   * Starts the SourceReader.  Stops when all sources are completed.
   */
  async start() {
    let counter = 0

    // Worker function
    const getNextLog = async (source, index, key) => {
      const result = {
        index,
        data: await source.popAsync()
      }

      // Dont forget to clean up the entry from the worker map...
      this.workers.delete(key)
      return result
    }
    // -----

    // Ensure we have an entry from every source initially before
    // we start processing en masse.
    this.sources.forEach((source, index) => {
      const key = counter++
      this.workers.set(key, getNextLog(source, index, key))
    })

    while (!this.allSourcesDrained()) {
      // When it's ready, process it.
      const result = await Promise.any(this.workers.values())

      // Sources return false when drained.
      if (result.data === false) {
        this.emit('drained', result.index)
        // Not much to do here...

      } else {
        this.emit('data', result)

        // Get the next log from the source we just sent out for
        // processing.
        const key = counter++
        const index = result.index
        this.workers.set(key, getNextLog(this.sources[index], index, key))
      }
    }

    this.emit('done')
  }
}


/**
 * We could refactor this into a more functional code style below,
 * but encapsulating this in a simple class felt cleaner.
 */
class LogSorter {
  /**
   * Contains logs, separated into buckets by time.
   * Currently, the timebox is one day.
   * 
   * `Map<number, Array>`
   * 
   *  `number` - A Number representing the date of the form `YYYYMMDD`.
   * 
   *  `Array` - An array of log entries for this timebox.
   */
  timeBuckets = new Map()

  /**
   * Contains the last bucket each source used.  Once all sources
   * are done with a given bucket, it can be shipped.
   * 
   * `Map<number, number>`
   *  
   *  `number` - The `sourceId` property of the entry 
   * 
   *  `number` - The `bucketId` of the 
   */
  lastBucketUsedBySource = new Map()

  /**
   * @param {SourceReader} reader 
   *  The `SourceReader` that we want to sort.
   * 
   * @param {Printer} printer 
   *  The provided `Printer`.
   */
  constructor(reader, printer) {
    this.reader = reader
    this.printer = printer

    this.reader.on('data', this.onData.bind(this))
    this.reader.on('drained', this.onDrained.bind(this))
  }


  /**
   * Start sorting logs from the `SourceReader`
   * @returns {Promise}
   *  A resolved promise once the `SourceReader` has completed.
   */
  async start() {
    // We stop when the reader stops.
    return new Promise((resolve, reject) => {
      this.reader.on('done', () => {
        resolve()
      })
    })
  }

  onDrained(index) {
    // // For testing
    // console.log(`Source ${index} has drained`)
  }

  onData(entry) {
    // Whenever we get a new entry from our SourceReader, we want to
    // add it to the appropriate timebox.
    const bucketId = this.dateToBucketId(entry.data.date)
    this.addLogToBucket(bucketId, entry)
    
    // Let the LogSorter know what the date was of this entry --
    // We can determine what timeboxes we're done with by determining
    // what timebox each source is currently in.
    this.lastBucketUsedBySource.set(entry.index, bucketId)
    const readyBuckets = this.getReadyBuckets()

    // When a bucket is ready for processing; sort it and print it!
    let bucket = undefined
    readyBuckets.forEach((bucketId) => {

      bucket = this.timeBuckets.get(bucketId)
      this.sortBucketByTimestamp(bucket)
      this.printBucket(bucket)

      // Don't forget to clean up finished buckets...
      this.timeBuckets.delete(bucketId)
    })
  }


  /**
   * Converts the log entry date into its' bucket index.
   * 
   * @param {string} date 
   *  The timestamp of the log entry.
   * @returns {Number}
   *  A Number with value `YYYYMMDD`
   */
  dateToBucketId(date) {
    let d = new Date(date)

    // 0-pad Month and Day
    let YYYY = d.getUTCFullYear()
    let MM = (d.getUTCMonth() + 1).toString().padStart(2, '0')
    let DD = d.getUTCDate().toString().padStart(2, '0')

    return Number(`${YYYY}${MM}${DD}`)
  }

  /**
   * Adds an entry to the appropriate bucket, or makes
   * a new bucket.  Currently, each bucket contains logs
   * from one day.
   * 
   * @param {Number} bucketId
   *  The bucketId of the timebox for the entry.
   * 
   * @param {Object} entry
   *  An Object with shape: { index, data }
   *    index: The index of the Source.
   *    data: The log entry.
   */
  addLogToBucket(bucketId, entry) {

    this.timeBuckets.has(bucketId) ? 
      this.timeBuckets.get(bucketId).push(entry.data)
      : this.timeBuckets.set(bucketId, [entry.data])
  }

  /**
   * Determines if any bucket is ready for further processing.
   * 
   * @returns {Array}
   *  An array of `bucketId`s that are done and ready for processing.
   */
  getReadyBuckets() {

    // We keep track of the last known bucket a source pushed into.  Since our
    // bucketIds are dates, any buckets with values less than the earliest date
    // are ready for processing.
    const earliestDate = [...this.lastBucketUsedBySource.values()].reduce((earliest, current) => {
      return current < earliest ? current : earliest
    })

    return [...this.timeBuckets.keys()].filter((bucketId) => {
      return bucketId < earliestDate
    })
  }

  /**
   * Sorts a bucket by the log's timestamp.
   * 
   * @param {Array} bucket 
   *  The bucket of logs to sort.
   */
  sortBucketByTimestamp(bucket) {
    bucket.sort((a, b) => {
      return a.date - b.date
    })
  }

  /**
   * Prints a bucket
   * 
   * [Note] The bucket should be sorted prior to printing.
   * 
   * @param {Array} bucket 
   *  The bucket of logs to print.
   */
  printBucket(bucket) {
    bucket.forEach(entry => {
      this.printer.print(entry)
    })
  }
}


// Print all entries, across all of the *async* sources, in chronological order.
module.exports = async (logSources, printer) => {
  // Start the reader engine...
  const reader = new SourceReader(logSources)
  reader.start()

  // Start sorting and printing our logs...
  const sorter = new LogSorter(reader, printer)
  await sorter.start()

  // Get dem stats!
  printer.done()
  console.log("Async sort complete.")
};