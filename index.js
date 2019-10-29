const moment = require("moment");

const jsonData = {
  leads: [
    {
      _id: "jkj238238jdsnfsj23",
      email: "foo@bar.com",
      firstName: "John",
      lastName: "Smith",
      address: "123 Street St",
      entryDate: "2014-05-07T17:30:20+00:00"
    },
    {
      _id: "edu45238jdsnfsj23",
      email: "mae@bar.com",
      firstName: "Ted",
      lastName: "Masters",
      address: "44 North Hampton St",
      entryDate: "2014-05-07T17:31:20+00:00"
    },
    {
      _id: "wabaj238238jdsnfsj23",
      email: "bog@bar.com",
      firstName: "Fran",
      lastName: "Jones",
      address: "8803 Dark St",
      entryDate: "2014-05-07T17:31:20+00:00"
    },
    {
      _id: "jkj238238jdsnfsj23",
      email: "coo@bar.com",
      firstName: "Ted",
      lastName: "Jones",
      address: "456 Neat St",
      entryDate: "2014-05-07T17:32:20+00:00"
    },
    {
      _id: "sel045238jdsnfsj23",
      email: "foo@bar.com",
      firstName: "John",
      lastName: "Smith",
      address: "123 Street St",
      entryDate: "2014-05-07T17:32:20+00:00"
    },
    {
      _id: "qest38238jdsnfsj23",
      email: "foo@bar.com",
      firstName: "John",
      lastName: "Smith",
      address: "123 Street St",
      entryDate: "2014-05-07T17:32:20+00:00"
    },
    {
      _id: "vug789238jdsnfsj23",
      email: "foo1@bar.com",
      firstName: "Blake",
      lastName: "Douglas",
      address: "123 Reach St",
      entryDate: "2014-05-07T17:33:20+00:00"
    },
    {
      _id: "wuj08238jdsnfsj23",
      email: "foo@bar.com",
      firstName: "Micah",
      lastName: "Valmer",
      address: "123 Street St",
      entryDate: "2014-05-07T17:33:20+00:00"
    },
    {
      _id: "belr28238jdsnfsj23",
      email: "mae@bar.com",
      firstName: "Tallulah",
      lastName: "Smith",
      address: "123 Water St",
      entryDate: "2014-05-07T17:33:20+00:00"
    },
    {
      _id: "jkj238238jdsnfsj23",
      email: "bill@bar.com",
      firstName: "John",
      lastName: "Smith",
      address: "888 Mayberry St",
      entryDate: "2014-05-07T17:33:20+00:00"
    }
  ]
};

// Take a variable number of identically structured json records and de-duplicate the set.

//  An example file of records is given in the accompanying 'leads.json'. Output should be same format, with dups reconciled according to the following rules:

//  1. The data from the newest date should be preferred

// 2. duplicate IDs count as dups. Duplicate emails count as dups. Both must be unique in our dataset. Duplicate values elsewhere do not count as dups.

// 3. If the dates are identical the data from the record provided last in the list should be preferred

//  Simplifying assumption: the program can do everything in memory (don't worry about large files)

//  The application should also provide a log of changes including some representation of the source record, the output record and the individual field changes (value from and value to) for each field.

//  Please implement as a command-line java or javascript program.

// id and emails

class DeDuplicates {
  constructor() {
    this.idLogs = [];
    this.emailLogs = [];
  }

  groupByValue(data, type) {
    const groupedData = data.reduce(function(obj, item) {
      obj[item[type]] = obj[item[type]] || [];
      obj[item[type]].push(item);
      return obj;
    }, {});
    return groupedData;
  }

  processDuplicates(arr, type) {
    let recentObject = {};
    let recentDate = Number.MIN_VALUE;
    let finalIndex = Number.MIN_VALUE;
    arr.forEach((item, index) => {
      let timeInUnix = moment(item.entryDate).unix();
      if (timeInUnix >= recentDate) {
        recentDate = timeInUnix;
        recentObject = Object.assign({}, item);
        finalIndex = index;
      }
    });
    this.processLogs(type, finalIndex, arr);
    return [{ ...recentObject }];
  }

  removeDuplicates(data, type) {
    for (let key in data) {
      if (data[key].length > 1) {
        if (data[key].length > 1) {
          data[key] = this.processDuplicates(data[key], type);
        }
      }
    }
    return data;
  }

  convertToOriginal(arr) {
    const newData = [];
    for (let key in arr) {
      newData.push(arr[key][0]);
    }
    return newData;
  }

  processLogs(type, finalIndex, arr) {
    const newData = [];
    arr.map((item, index) => {
      if (index === finalIndex) {
        if (type === "_id") {
          newData.unshift(
            `RETAINED ID ==> ${JSON.stringify(arr[finalIndex])} `
          );
        } else if (type === "email") {
          newData.unshift(
            `RETAINED EMAIL ==> ${JSON.stringify(arr[finalIndex])} `
          );
        }
      } else {
        if (type === "_id") {
          newData.push(`DUPLICATE ID ==> ${JSON.stringify(arr[index])} `);
        } else if (type === "email") {
          newData.push(`DUPLICATE EMAIL ==> ${JSON.stringify(arr[index])} `);
        }
      }
    });
    if (type === "_id") {
      this.idLogs.push(newData);
    } else if (type === "email") {
      this.emailLogs.push(newData);
    }
  }
}

const deDuplicates = new DeDuplicates();

const groupdById = deDuplicates.groupByValue(jsonData.leads, "_id");
const removedDuplicteIds = deDuplicates.removeDuplicates(groupdById, "_id");
const convetedToOriginal = deDuplicates.convertToOriginal(removedDuplicteIds);
const groupdByEmail = deDuplicates.groupByValue(convetedToOriginal, "email");
const removedDuplicteEmails = deDuplicates.removeDuplicates(
  groupdByEmail,
  "email"
);
const convetedToOriginalFinal = deDuplicates.convertToOriginal(
  removedDuplicteEmails
);

console.log(
  "******************************************************************************************"
);

console.log("ID LOGS", JSON.stringify( deDuplicates.idLogs, null, 2));
console.log(
  "******************************************************************************************"
);

console.log("EMAIL LOGS", JSON.stringify( deDuplicates.emailLogs, null, 2));
console.log(
  "******************************************************************************************"
);

console.log("FINAL RESULT", JSON.stringify(convetedToOriginalFinal, null, 2));
