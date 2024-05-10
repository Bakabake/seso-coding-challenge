"use strict";


/**
 * Adds a source entry to the funnel, ensuring the new entry
 * is sorted into the funnel.  Uses binary insertion sort.
 * Modifies the original array.
 * 
 * @param {Object} entry 
 *  The log entry to be added.
 *  Contains the source index and payload of the most recent entry 
 *  from that source.
 *
 * @param {Array} funnel 
 *  The array containing entries from every source.
 */
const addSourceEntryToFunnel = (entry, funnel) => {
  let start = 0
  let end = funnel.length - 1
  let mid = undefined

  while (start <= end) {
    mid = (start + end) >> 1
    // Which half is it in?
    if (entry.data.date < funnel[mid].data.date) {
      // The 'before' half.
      end = mid - 1
    } else if (entry.data.date > funnel[mid].data.date) {
      // The 'after' half.
      start = mid + 1
    } else {
      // Timestamps are identical.
      start = mid
      break
    }
  }

  funnel.splice(start, 0, entry)
}

/**
 * The funnel is responsible for maintaining one entry from every source.
 * 
 * @param {Array} logSources 
 *  The array of sources we're ingesting
 * 
 * @returns {Array} 
 *  An array that contains the initial data in the funnel, sorted chronologically.
 */
const initializeFunnel = async (logSources) => {

  const entries = new Array()
  for (let i = 0; i < logSources.length; ++i) {
    entries.push({
      source: i,
      data: await logSources[i].popAsync()
    })
  }

  // After we've grabbed the first entry from each source, we want to do one big
  // initial sort of the data.
  return entries.sort((a, b) => {
    return a.data.date - b.data.date
  })
}


// Print all entries, across all of the *async* sources, in chronological order.
module.exports = async (logSources, printer) => {

  // --------------------------------------------------------------------------
  // Helper function to determine when all sources are drained.
  const allSourcesDrained = () => {
    return logSources.reduce((acc, currentValue) => acc && currentValue.drained, true)
  }

  // Helper function to process entries from an async source.
  const processEntries = async (funnel) => {
    if (!allSourcesDrained()) {
      let currentEntry = funnel.shift()
      printer.print(currentEntry.data)

      // When we remove an entry, replenish the funnel with an entry
      // from the source we just processed.  However, if the source
      // has been drained, we'll just get `false`.
      const data = await logSources[currentEntry.source].popAsync()
      const newEntry = {
        source: currentEntry.source,
        data
      }
      if (Boolean(newEntry.data)) {
        addSourceEntryToFunnel(newEntry, funnel)
      }

      await processEntries(funnel)
    }
  }
  // ------------------------------------------------------------------------


  let funnel = await initializeFunnel(logSources)
  await processEntries(funnel, logSources)

  // Get dem stats
  printer.done()
  console.log("Async sort complete.")
};