const CryptoJS = require('crypto-js');

const generatePromisAuthToken = async () => {
    const { getSecret } = require('./shared');

    const uoid = await getSecret(process.env.PROMIS_UOID);
    const token = await getSecret(process.env.PROMIS_TOKEN);

    const encodedWord = CryptoJS.enc.Utf8.parse(uoid + ":" + token);
    const encoded = CryptoJS.enc.Base64.stringify(encodedWord);
    return encoded;
}

const processPromisResults = async (uid) => {
    
    const { surveyExists, updateSurvey } = require('./firestore');

    const collection = 'promis_v1';
    const doc = await surveyExists(collection, uid);
    const surveyResults = doc.data();

    const forms = Object.keys(promisConfig);
    const scoresPayload = {};
    const scoresPromises = [];

    const token = await generatePromisAuthToken();

    for (let form of forms) {
        const sourceQuestion = surveyResults[promisConfig[form].source];

        if (sourceQuestion) {

            let scoringData = {};

            const questions = Object.keys(promisConfig[form].questions);

            for (const question of questions) {
                if (sourceQuestion[question]) {

                    const questionConfig = promisConfig[form].questions[question];
                    const questionResponse = sourceQuestion[question];

                    scoringData[questionConfig.name] = questionConfig.responses[questionResponse];
                }
            }

            scoresPromises.push(
                getScoringData(promisConfig[form].id, scoringData, token).then(scores => {
                    if (scores) {
                        const tScore = parseInt(scores['T-Score']);
                        const sError = parseInt(scores['SError']);
                        
                        // Only save values that are valid numbers (not NaN)
                        if (!isNaN(tScore)) {
                            scoresPayload[promisConfig[form].score] = tScore;
                        }
                        
                        if (!isNaN(sError)) {
                            scoresPayload[promisConfig[form].error] = sError;
                        }
                    }
                })
            );
        }
    }

    Promise.all(scoresPromises).then(async () => {
        if (Object.keys(scoresPayload).length > 0) {
            await updateSurvey(scoresPayload, collection, doc);
        }
    }).catch(error => {
        console.error("Error in processing scoring data:", error);
    });
}

const getScoringData = async (id, data, token) => {

    const formData = new URLSearchParams();
    const url = `https://dcb-promis.cit.nih.gov/2013-01/Scores/${id}.json`;
    
    Object.keys(data).forEach(key => {
        formData.append(key, data[key]);
    });

    try {
        const response = await fetch(url, {
            method: "POST",
            headers: {
                "Authorization": `Basic ${token}`,
                "Content-Type": "application/x-www-form-urlencoded"
            },
            body: formData.toString()
        });

        const scores = await response.json();

        if (scores.ItemErrors) {
            throw new Error(`Errors in request data sent to PROMIS Scoring API: ${scores.ItemErrors}`);
        }

        return scores.Form[0];
    }
    catch (error) {
        console.error('Error fetching PROMIS Scoring Data: ', error);
        return null;
    }
}

const promisResponseSets = {
    responseSet1: {
        '367964536': 'Without any difficulty',
        '193058048': 'With a little difficulty',
        '263412634': 'With some difficulty',
        '734992634': 'With much difficulty',
        '221192556': 'Unable to do'
    },
    responseSet2: {
        '648960871': 'Never',
        '235613392': 'Rarely',
        '132099255': 'Sometimes',
        '211590917': 'Often',
        '907579123': 'Always'
    },
    responseSet3: {
        '111520945': 'Not at all',
        '548628123': 'A little bit',
        '567908725': 'Somewhat',
        '760969884': 'Quite a bit',
        '464631026': 'Very much'
    },
    responseSet4: {
        '878535894': 'Very poor',
        '138752522': 'Poor',
        '131550264': 'Fair',
        '719933364': 'Good',
        '565881164': 'Very good'
    },
    responseSet5: {
        '648960871': 'Never',
        '235613392': 'Rarely',
        '132099255': 'Sometimes',
        '734611348': 'Usually',
        '907579123': 'Always'
    },
    responseSet6: {
        '648960871': 'Never',
        '235613392': 'Rarely (Once)',
        '132099255': 'Sometimes (Two or three times)',
        '211590917': 'Often (About once a day)',
        '398006869': 'Very often (Several times a day)'
    }
}

const promisConfig = {
    'Physical Function': {
        id: '7AA4002D-61C1-438D-9063-91A17FFA68BB',
        source: 'D_284353934',
        questions: {
            'D_559540891': {
                name: 'PFA11',
                responses: promisResponseSets.responseSet1
            }, 
            'D_917425212': {
                name: 'PFA21',
                responses: promisResponseSets.responseSet1
            }, 
            'D_783201540': {
                name: 'PFA23',
                responses: promisResponseSets.responseSet1
            }, 
            'D_780866928': {
                name: 'PFA53',
                responses: promisResponseSets.responseSet1
            }
        },
        score: 'D_608953994',
        error: 'D_582985641'
    },
    'Anxiety': {
        id: '6C13662F-179A-45AD-A8DF-16843D902E28',
        source: 'D_404946076',
        questions: {
            'D_429680247': {
                name: 'EDANX01',
                responses: promisResponseSets.responseSet2
            }, 
            'D_179665441': {
                name: 'EDANX40',
                responses: promisResponseSets.responseSet2
            }, 
            'D_195292223': {
                name: 'EDANX41',
                responses: promisResponseSets.responseSet2
            }, 
            'D_829976839': {
                name: 'EDANX53',
                responses: promisResponseSets.responseSet2
            }
        },
        score: 'D_328149278',
        error: 'D_281351288'
    },
    'Depression': {
        id: '597A9B24-5B5C-487D-9606-451355DC6E3D',
        source: 'D_715048033',
        questions: {
            'D_468039454': {
                name: 'EDDEP04',
                responses: promisResponseSets.responseSet2
            }, 
            'D_361835532': {
                name: 'EDDEP06',
                responses: promisResponseSets.responseSet2
            }, 
            'D_803322918': {
                name: 'EDDEP29',
                responses: promisResponseSets.responseSet2
            }, 
            'D_168582270': {
                name: 'EDDEP41',
                responses: promisResponseSets.responseSet2
            }
        },
        score: 'D_297016093',
        error: 'D_144446277'
    },
    'Fatigue': {
        id: '2E25400B-9373-4631-B769-2E4D466EED04',
        source: 'D_907490539',
        questions: {
            'D_467404576': {
                name: 'HI7',
                responses: promisResponseSets.responseSet3
            }, 
            'D_658945347': {
                name: 'AN3',
                responses: promisResponseSets.responseSet3
            }, 
            'D_105063268': {
                name: 'FATEXP41',
                responses: promisResponseSets.responseSet3
            }, 
            'D_787436735': {
                name: 'FATEXP40',
                responses: promisResponseSets.responseSet3
            }
        },
        score: 'D_410751656',
        error: 'D_101862879'
    },
    'Sleep Disturbance': {
        id: '795B07C1-067E-4FBD-9B60-A57985E69B5D',
        source: 'D_336566965',
        questions: {
            'D_992194402': {
                name: 'Sleep109',
                responses: promisResponseSets.responseSet4
            }, 
            'D_624200915': {
                name: 'Sleep116',
                responses: promisResponseSets.responseSet3
            }, 
            'D_526006101': {
                name: 'Sleep20',
                responses: promisResponseSets.responseSet3
            }, 
            'D_644233792': {
                name: 'Sleep44',
                responses: promisResponseSets.responseSet3
            }
        },
        score: 'D_990793746',
        error: 'D_218828500'
    },
    'Social Participation': {
        id: 'B730F00E-C958-4654-85C1-0931A7289322',
        source: 'D_420392309',
        questions: {
            'D_271090432': {
                name: 'SRPPER11_CaPS',
                responses: promisResponseSets.responseSet5
            }, 
            'D_828608766': {
                name: 'SRPPER18_CaPS',
                responses: promisResponseSets.responseSet5
            }, 
            'D_886047084': {
                name: 'SRPPER23_CaPS',
                responses: promisResponseSets.responseSet5
            }, 
            'D_226478149': {
                name: 'SRPPER46_CaPS',
                responses: promisResponseSets.responseSet5
            }
        },
        score: 'D_393186700',
        error: 'D_801982443',
    },
    'Pain Interference': {
        id: 'A192B2B3-9FB0-40A4-9E95-AF309780C8E0',
        source: 'D_420560514',
        questions: {
            'D_693503159': {
                name: 'PAININ9',
                responses: promisResponseSets.responseSet3
            }, 
            'D_754781311': {
                name: 'PAININ22',
                responses: promisResponseSets.responseSet3
            }, 
            'D_380975443': {
                name: 'PAININ31',
                responses: promisResponseSets.responseSet3
            }, 
            'D_426764225': {
                name: 'PAININ34',
                responses: promisResponseSets.responseSet3
            }
        },
        score: 'D_370227545',
        error: 'D_766974306',
    },
    'Social Satisfaction': {
        id: '2AF69E2C-7571-475B-A5DE-E77AF1DF4A17',
        source: 'D_261177801',
        questions: {
            'D_313287837': {
                name: 'SRPSAT06r1',
                responses: promisResponseSets.responseSet3
            }, 
            'D_598001940': {
                name: 'SPRSAT34r1',
                responses: promisResponseSets.responseSet3
            }, 
            'D_733106290': {
                name: 'SRPSAT34r1',
                responses: promisResponseSets.responseSet3
            }, 
            'D_542492755': {
                name: 'SRPSAT49r1',
                responses: promisResponseSets.responseSet3
            }
        },
        score: 'D_697423629',
        error: 'D_424548783',
    },
    'Social Isolation': {
        id: 'BE5C09D1-1FB2-474C-B17C-8EC8F98F396D',
        source: 'D_326712049',
        questions: {
            'D_437905191': {
                name: 'UCLA11x2',
                responses: promisResponseSets.responseSet5
            }, 
            'D_387564567': {
                name: 'UCLA13x3',
                responses: promisResponseSets.responseSet5
            }, 
            'D_479680555': {
                name: 'UCLA14x2',
                responses: promisResponseSets.responseSet5
            }, 
            'D_311938392': {
                name: 'UCLA18x2',
                responses: promisResponseSets.responseSet5
            }
        },
        score: 'D_129202138',
        error: 'D_633951291',
    },
    'Cognitive Function': {
        id: 'A0511754-DFB1-4492-81D9-1FC3ED3DD31C',
        source: 'D_230486322',
        questions: {
            'D_960308206': {
                name: 'PC2r',
                responses: promisResponseSets.responseSet6
            }, 
            'D_974618086': {
                name: 'PC35r',
                responses: promisResponseSets.responseSet6
            }, 
            'D_115480943': {
                name: 'PC36r',
                responses: promisResponseSets.responseSet6
            }, 
            'D_650091514': {
                name: 'PC42r',
                responses: promisResponseSets.responseSet6
            }
        },
        score: 'D_755066850',
        error: 'D_132343154',
    }
}

module.exports = {
    processPromisResults
}