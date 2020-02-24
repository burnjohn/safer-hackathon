const path = require('path');
const fs = require('fs');
const csv = require('csvtojson');
const {Parser} = require('json2csv');

const categories = require('./categories.json');

const outputFilePathTest = path.join(__dirname, '../', 'output-datasets',
  'extended-patient-test.csv',
);
const outputFilePathTrain = path.join(__dirname, '../', 'output-datasets',
  'extended-patient-train.csv',
);

const diagnosisPath = path.join(__dirname, '../', 'input-datasets',
  'SyncDiagnosis.csv',
);
const patientPath = path.join(__dirname, '../', 'input-datasets',
  'SyncPatient_train.csv',
);
const patientTestPath = path.join(__dirname, '../', 'input-datasets',
  'SyncPatient_test.csv',
);
const disctinctYearPath = path.join(__dirname, '../', 'input-datasets',
  'number_of_distinct_predictionyear_with_same_medication.csv',
);
const syncMedicationUnitsConvertedPath = path.join(__dirname, '../',
  'input-datasets', 'SyncMedication_units_converted.csv',
);
const syncTranscriptPath = path.join(__dirname, '../', 'input-datasets',
  'SyncTranscript.csv',
);

(async () => {
  const diagnosisDataset = await csv().fromFile(diagnosisPath);
  const disctinctYearDataset = await csv().fromFile(disctinctYearPath);

  const patientsDatasetTrain = await csv().fromFile(patientPath);
  const medicationUnitsConverted = await csv()
    .fromFile(syncMedicationUnitsConvertedPath);
  const transcriptDataset = await csv().fromFile(syncTranscriptPath);

  console.log('Start datasets processing');

  const newPatientsTrain = getProcessedPatients(patientsDatasetTrain,
    diagnosisDataset, disctinctYearDataset, medicationUnitsConverted,
    transcriptDataset,
  );

  saveFileToOutputs(outputFilePathTrain, newPatientsTrain);

  console.log('Patient train dataset is saved');

  const patientsDatasetTest = await csv().fromFile(patientTestPath);
  const newPatientsTest = getProcessedPatients(patientsDatasetTest,
    diagnosisDataset, disctinctYearDataset, medicationUnitsConverted,
    transcriptDataset,
  );

  saveFileToOutputs(outputFilePathTest, newPatientsTest);
  console.log('Patient test dataset is saved');
})();


let medicationNames = [];

function getProcessedPatients(
  patientDataset, diagnosisDataset, distinctYearDataset,
  medicationUnitsConverted, transcriptDataset) {
  const diagnosisByPatientMap = getDiagnosisByPatientMap(diagnosisDataset);
  const distinctYearByPatientMap = getDistinctYearByPatientMap(
    distinctYearDataset);
  const medicationsByPatientMap = getMedicationsByPatientMap(
    medicationUnitsConverted);
  const transcriptByPatientMap = getTranscriptByPatientMap(transcriptDataset);


  return patientDataset.map(patient => {
    const allPatientDiagnosis = diagnosisByPatientMap.get(
      patient['PatientGuid']);
    const allYearValues = distinctYearByPatientMap.get(
      patient['PatientGuid']) || {
      avg_number_distinct_year_medication: 0,
      max_number_distinct_year_medication: 0,
    };

    const allPatientMedication = medicationsByPatientMap.get(
      patient['PatientGuid']);

    const allPatientTranscript = transcriptByPatientMap.get(patient['PatientGuid']);

    const patientDiagnosisDict = allPatientDiagnosis.reduce(
      (acc, diagnosis) => {
        const diagnosisKeyName = `[Diagnosis]_${diagnosis['ICD9Code']}`;
        const currCount = acc[diagnosis['name_by_ICD9']] || 0;

        acc[diagnosis['name_by_ICD9']] = currCount + 1;
        acc[diagnosisKeyName] = 1;

        return acc;
      }, {});

    const patientMedicationDict = getMinMaxMedicationStrength(
      allPatientMedication);

    const patientTranscriptDict = getMinMaxTranscriptValues(allPatientTranscript);

    return {
      ...patient,
      ...allYearValues,
      'patient_diagnosis_dict': JSON.stringify(patientDiagnosisDict),
      'patient_medication_dict': JSON.stringify(patientMedicationDict),
      'transcript_medication_dict': JSON.stringify(patientTranscriptDict),
    };
  });
}

function getTranscriptByPatientMap(transcriptDataset) {
  const transcriptByUserMap = new Map();

  transcriptDataset.forEach(transcript => {
    const patientId = transcript['patientGuid'];

    const allPatientTranscripts = transcriptByUserMap.get(patientId) || [];

    transcriptByUserMap.set(patientId, [...allPatientTranscripts, transcript]);
  });

  return transcriptByUserMap;
}

function getMedicationsByPatientMap(medicationUnitsConverted) {
  const uniqueMedications = new Set();
  medicationUnitsConverted.forEach(
    med => uniqueMedications.add(med['MedicationName']));

  medicationNames = Array.from(uniqueMedications);

  const medicationByPatientMap = new Map();

  medicationUnitsConverted.forEach(medication => {
    const patientId = medication['PatientGuid'];
    const medName = medication['MedicationName'];
    const medStrength = medication['MedicationStrength'];

    const patientMedications = medicationByPatientMap.get(patientId);

    if (!patientMedications) {
      medicationByPatientMap.set(patientId,
        generateMedicationFeatures(medName, medStrength),
      );
      return;
    }

    if (!patientMedications[`[medication-name]_${medName}_count`]) {
      addMedicationFeature(patientMedications, medName, medStrength);
    }

    const currentMedicationCount = patientMedications &&
      patientMedications[`[medication-name]_${medName}_count`];

    patientMedications[`[medication-name]_${medName}_count`] = currentMedicationCount +
      1;

    if (medStrength) {
      patientMedications[`[medication-name]_${medName}_STRENGTH_ALL`].push(
        medStrength);
    }
  });

  return medicationByPatientMap;
}

function generateMedicationFeatures(medName, medStrength) {
  return {
    [`[medication-name]_${medName}_count`]: 1,
    [`[medication-name]_${medName}_STRENGTH_ALL`]: [medStrength],
  };
}

function addMedicationFeature(feature, medName, medStrength) {
  feature[`[medication-name]_${medName}_count`] = 1;
  feature[`[medication-name]_${medName}_STRENGTH_ALL`] = [medStrength];
}

function getMinMaxMedicationStrength(patient) {
  medicationNames.forEach(medName => {
    const patientHasMedication = patient &&
      patient[`[medication-name]_${medName}_count`] &&
      patient[`[medication-name]_${medName}_count`] > 0;

    if (patientHasMedication) {
      const allStrengthList = patient[`[medication-name]_${medName}_STRENGTH_ALL`];

      delete patient[`[medication-name]_${medName}_STRENGTH_ALL`];

      if (!allStrengthList) {
        return;
      }

      const filteredValues = allStrengthList.filter(str => !!str || str === 0);

      if (filteredValues.length) {
        const avg = getAvg(filteredValues);
        const min = getMin(filteredValues);
        const max = getMax(filteredValues);

        patient[`[medication-name]_${medName}_STRENGTH_MIN`] = min;
        patient[`[medication-name]_${medName}_STRENGTH_AVG`] = avg;
        patient[`[medication-name]_${medName}_STRENGTH_MAX`] = max;
      }
    }
  });

  return patient;
}

const valuesList = ['height', 'weight', 'bmi', 'systolicBP', 'diastolicBP', 'respiratoryRate', 'heartRate'];

function getMinMaxTranscriptValues(patientTranscriptsList) {
  const patient = {};

  valuesList.forEach(value => {
    const valuesList = patientTranscriptsList.map( transcript => Number(transcript[value]) ).filter( transcript => !!transcript);

    if (!valuesList.length) return;

    patient[`patient_${value}_MIN`] = getMin(valuesList);
    patient[`patient_${value}_AVG`] = getAvg(valuesList);
    patient[`patient_${value}_MAX`] = getMax(valuesList);
  });

  return patient;
}

function getDistinctYearByPatientMap(distinctYearDataset) {
  const distinctYearDatasetMap = new Map();

  distinctYearDataset.forEach(value => {
    const {PatientGuid, avg_number_distinct_year_medication, max_number_distinct_year_medication} = value;

    distinctYearDatasetMap.set(PatientGuid, {
      avg_number_distinct_year_medication: Number(
        avg_number_distinct_year_medication) || 0,
      max_number_distinct_year_medication: Number(
        max_number_distinct_year_medication) || 0,
    });
  });

  return distinctYearDatasetMap;
}

function getDiagnosisByPatientMap(diagnosisDataset) {
  const diagnosisByPatientMap = new Map();

  diagnosisDataset.forEach(diagnosis => {
    const patientDiagnosisList = diagnosisByPatientMap.get(
      diagnosis['PatientGuid']) || [];

    const diagnosisCategoryName = getDiagnosisCategory(diagnosis);

    diagnosis['name_by_ICD9'] = diagnosisCategoryName;

    diagnosisByPatientMap.set(diagnosis['PatientGuid'],
      [...patientDiagnosisList, diagnosis],
    );

  });

  return diagnosisByPatientMap;
}


function getDiagnosisCategory(diagnosis) {
  const normalizedICD9Categories = Object.keys(categories).reduce((acc, name) => {
    const snakeCaseName = '[diagnosis_count]_' + name.split(' ').join('_');
    acc[snakeCaseName] = categories[name];
    return acc;
  }, {});

  const categoriesList = Object.entries(normalizedICD9Categories);

  const ICD9Code = diagnosis['ICD9Code'];
  const numberICDCode = Math.ceil(Number(diagnosis['ICD9Code']));

  if (!numberICDCode) {
    const codeStr = String(ICD9Code).toUpperCase();

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
    return null;
  }

  return category[0];
}

// Utility functions

function getAvg(values) {
  if (values && values.length === 1) {
    return Number(values[0]);
  }
  const sum = values.reduce((sum, val) => {
    return sum + Number(val);
  }, 0);

  return sum / values.length;
}

function getMin(values) {
  if (values && values.length === 1) {
    return Number(values[0]);
  }

  return Math.min(...values) || 'NULL';
}

function getMax(values) {
  if (values && values.length === 1) {
    return Number(values[0]);
  }

  return Math.max(...values) || 'NULL';
}

function saveFileToOutputs(path, data) {
  const parser = new Parser({fields: Object.keys(data[0])});
  const outputCsv = parser.parse(data);

  fs.writeFileSync(path, outputCsv);
}