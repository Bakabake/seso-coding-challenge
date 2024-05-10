"use strict";


/**
 * Adds a source entry to the funnel, ensuring the new entry
 * is sorted into the funnel.  Uses binary insertion sort.
 * Modifies the original array.
 * 
 * @param {Object} log 
 *  The log entry to be added.
 *  Contains the source index and payload of the most recent entry 
 *  from that source.
 *
 * @param {Array} funnel 
 *  The array containing entries from every source.
 */
const addSourceEntryToFunnel = (log, funnel) => {
  let start = 0
  let end = funnel.length - 1
  let mid = undefined

  while (start <= end) {
    mid = (start + end) >> 1
    // Which half is it in?
    if (log.data.date < funnel[mid].data.date) {
      // The 'before' half.
      end = mid - 1
    } else if (log.data.date > funnel[mid].data.date) {
      // The 'after' half.
      start = mid + 1
    } else {
      // Timestamps are identical.
      start = mid
      break
    }
  }

  funnel.splice(start, 0, log)
}


// Print all entries, across all of the sources, in chronological order.
module.exports = (logSources, printer) => {

  // Helper function to determine when all sources are drained.
  const allSourcesDrained = () => {
    return logSources.reduce((acc, currentValue) => acc && currentValue.drained, true)
  }
  // -----

  
  // The funnel is responsible for maintaining one entry from every source.
  // After we've grabbed the first entry from each source, we want to do one big
  // initial sort of the data.
  let funnel = logSources.map((source, index) => {
    return {
      source: index,
      data: source.pop()
    }
  }).sort((a, b) => {
    return a.data.date - b.data.date
  })

  // Now we have every chronologically-ordered entry from each source,
  // sorted chronologically.  
  // Let's start processing the data in our funnel.
  let currentEntry = undefined
  let newEntry = undefined
  while (!allSourcesDrained()) {
    // Get the next entry and print it!
    currentEntry = funnel.shift()
    printer.print(currentEntry.data)

    // When we remove an entry, replenish the funnel with an entry
    // from the source we just processed.  However, if the source
    // has been drained, we'll just get `false`.
    newEntry = {
      source: currentEntry.source,
      data: logSources[currentEntry.source].pop()
    }
    if (Boolean(newEntry.data)) {
      addSourceEntryToFunnel(newEntry, funnel)
    }
  }

  // Get dem stats
  printer.done()
  return console.log("Sync sort complete.");
};
