const path = require('path');
const fs = require('fs');
const csv = require('csvtojson');
const { Parser } = require('json2csv');

const categories = require('./categories.json');

const outputFilePathTest = path.join(__dirname, '../', 'output-datasets','extended-patient-test.csv');
const outputFilePathTrain = path.join(__dirname, '../', 'output-datasets','extended-patient-train.csv');

const diagnosisPath = path.join(__dirname, '../','input-datasets', 'SyncDiagnosis.csv');
const patientPath = path.join(__dirname, '../','input-datasets', 'SyncPatient_train.csv');
const disctinctYearPath = path.join(__dirname, '../','input-datasets', 'number_of_distinct_predictionyear_with_same_medication.csv');

const normalizedCategories = Object.keys(categories).reduce( (acc, name) => {
  const snakeCaseName = '[diagnosis_count]_'+ name.split(' ').join('_');
  acc[snakeCaseName] = categories[name];
  return acc;
}, {});

const categoriesList = Object.entries(normalizedCategories);

const defaultCategories = categoriesList.reduce( (acc, [name]) => {
  acc = {...acc, [name]: 0};

  return acc;
}, {});

(async () => {
  const diagnosisDataset = await csv().fromFile(diagnosisPath);
  const disctinctYearDataset = await csv().fromFile(disctinctYearPath);

  const patientsDatasetTrain = await csv().fromFile(patientPath);
  const newPatientsTrain = getProcessedPatients(patientsDatasetTrain, diagnosisDataset, disctinctYearDataset);

  saveFileToOutputs(outputFilePathTrain, newPatientsTrain);

  console.log('Train dataset is saved');

  const patientsDatasetTest = await csv().fromFile(patientPath);
  const newPatientsTest = getProcessedPatients(patientsDatasetTest, diagnosisDataset, disctinctYearDataset);
  saveFileToOutputs(outputFilePathTest, newPatientsTest);
  console.log('Test dataset is saved');
})();

function getProcessedPatients(patientDataset, diagnosisDataset, distinctYearDataset) {
  const diagnosisByPatientMap = getDiagnosisByPatienMap(diagnosisDataset);
  const distinctYearByPatientMap = getDistinctYearByPatientMap(distinctYearDataset);

  return patientDataset.map( patient => {
    const allPatientDiagnosis = diagnosisByPatientMap.get(patient['PatientGuid']);
    const allYearValues = distinctYearByPatientMap.get(patient['PatientGuid']) || {
      avg_number_distinct_year_medication: 0,
      max_number_distinct_year_medication: 0
    };

    const patientDiagnosisDict = allPatientDiagnosis.reduce( (acc, diagnosis) => {
      const currCount = acc[diagnosis['name_by_ICD9']] || 0;

      acc[diagnosis['name_by_ICD9']] = currCount +1;

      return acc;
    }, {});


    return {
      ...patient,
      ...defaultCategories,
      ...patientDiagnosisDict,
      ...allYearValues,
    }
  });
}

function getDistinctYearByPatientMap(distinctYearDataset) {
  const distinctYearDatasetMap = new Map();

  distinctYearDataset.forEach( value => {
    const {PatientGuid, avg_number_distinct_year_medication, max_number_distinct_year_medication} = value;

    distinctYearDatasetMap.set(PatientGuid, {
      avg_number_distinct_year_medication: Number(avg_number_distinct_year_medication) || 0,
      max_number_distinct_year_medication: Number(max_number_distinct_year_medication) || 0,
    });
  });

  return distinctYearDatasetMap;
}

function getDiagnosisByPatienMap(diagnosisDataset) {
  const diagnosisByPatientMap = new Map();

  diagnosisDataset.forEach( diagnosis => {
    const patientDiagnosisList = diagnosisByPatientMap.get(diagnosis['PatientGuid']) || [];

    const diagnosisCategoryName = getDiagnosisCategory(diagnosis);

    diagnosis['name_by_ICD9'] = diagnosisCategoryName;

    diagnosisByPatientMap.set(diagnosis['PatientGuid'], [...patientDiagnosisList, diagnosis]);

  });

  return diagnosisByPatientMap;
}

function getDiagnosisCategory(diagnosis) {
  const ICDCode = diagnosis['ICD9Code'];
  const numberICDCode = Math.ceil(Number(diagnosis['ICD9Code']));

  if (!numberICDCode) {
    const codeStr = String(ICDCode).toUpperCase();

    if (codeStr.includes('E') || codeStr.includes('V')) {
      const lastCategoryName = categoriesList[categoriesList.length - 1][0];
      return lastCategoryName;
    }
  }

  const category = categoriesList.find(([name, condition]) => {
    const [start, end] = condition;


    if (start <= numberICDCode && numberICDCode <= end) {
      return true;
    }
  });

  if (!category) {
    return null
  }

  return category[0];
}


function saveFileToOutputs(path, data) {
  const parser = new Parser({ fields: Object.keys(data[0]) });
  const outputCsv = parser.parse(data);

  fs.writeFileSync(path, outputCsv);
}