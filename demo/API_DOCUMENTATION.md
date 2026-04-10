# Connect Follow-Up APIs — Documentation

> **Status:** Demo / POC  
> **Environment:** Dev only  
> **Base URL:** `https://us-central1-nih-nci-dceg-connect-dev.cloudfunctions.net`

---

## Authentication

All endpoints require **service account OAuth2 authentication** via the `Authorization` header.

```
Authorization: Bearer <access_token>
```

The access token must belong to a service account registered in the `siteDetails` Firestore collection. The authenticated site determines which participants are visible (scoped by site code).

Requests without a valid token receive `401 Authorization failed!`.

---

## Endpoints

### 1. Get Follow-Up Eligibility

Returns activity records for participants eligible for follow-up rounds.

```
GET /followupEligibility
```

#### Query Parameters

| Parameter | Type   | Required | Description                                      |
|-----------|--------|----------|--------------------------------------------------|
| `token`   | string | No       | Participant token. If omitted, returns all eligible participants for the authenticated site. |

#### Response

**`200 OK`**

```json
{
  "data": [
    {
      "connectId": 5027907288,
      "token": "c3ed18b3-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      "round": "A1",
      "activityType": "SDS",
      "activities": [
        "Blood Collection",
        "Urine Collection",
        "Blood Urine Survey",
        "Incentive"
      ],
      "creationDate": "2026-04-09T17:30:00.000Z"
    },
    {
      "connectId": 5027907288,
      "token": "c3ed18b3-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      "round": "A2",
      "activityType": "CED",
      "activities": [
        "Blood Collection",
        "Urine Collection",
        "Mouthwash Collection",
        "Blood Urine Survey",
        "Mouthwash Survey"
      ],
      "creationDate": "2026-04-09T17:30:00.000Z"
    }
  ],
  "code": 200
}
```

#### Response Fields

| Field          | Type     | Description                                                              |
|----------------|----------|--------------------------------------------------------------------------|
| `connectId`    | number   | The participant's Connect ID.                                            |
| `token`        | string   | The participant's unique token.                                          |
| `round`        | string   | Activity round identifier (`A1`, `A2`, `A3`, `A4`).                      |
| `activityType` | string   | Round category — `SDS` (Same Day Sample) or `CED` (Clinical Exam Day).   |
| `activities`   | string[] | List of activities the participant is eligible for in this round.         |
| `creationDate` | string   | ISO 8601 timestamp of when the activity record was created.              |

#### Error Responses

| Status | Condition                              | Body                                                                 |
|--------|----------------------------------------|----------------------------------------------------------------------|
| `401`  | Missing or invalid authorization       | `{ "message": "Authorization failed!", "code": 401 }`               |
| `403`  | Called in stage or prod environment     | `{ "message": "API not available in this environment.", "code": 403 }` |
| `404`  | Token provided but not found for site  | `{ "message": "Participant not found or does not belong to your site.", "code": 404 }` |
| `405`  | Non-GET method used                    | `{ "message": "Only GET requests are accepted!", "code": 405 }`     |
| `500`  | Server error                           | `{ "message": "An error occurred. Please try again later.", "code": 500 }` |

#### Example

```bash
# All eligible participants for your site
curl -H "Authorization: Bearer $TOKEN" \
  "https://us-central1-nih-nci-dceg-connect-dev.cloudfunctions.net/followupEligibility"

# Single participant
curl -H "Authorization: Bearer $TOKEN" \
  "https://us-central1-nih-nci-dceg-connect-dev.cloudfunctions.net/followupEligibility?token=c3ed18b3-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

---

### 2. Submit Follow-Up Collections

Submit collection details for participants at your site. Each item in the request creates a collection record.

```
POST /followupCollections
```

#### Request Body

```json
{
  "data": [
    {
      "token": "c3ed18b3-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      "round": "A1",
      "type": "Blood",
      "status": "Complete",
      "date": "2026-04-09T10:30:00.000Z",
      "location": "HP Research Medical"
    },
    {
      "token": "c3ed18b3-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      "round": "A1",
      "type": "Urine",
      "status": "Complete",
      "date": "2026-04-09T10:35:00.000Z",
      "location": "HP Research Medical"
    }
  ]
}
```

#### Request Fields

| Field      | Type   | Required | Description                                                     |
|------------|--------|----------|-----------------------------------------------------------------|
| `token`    | string | **Yes**  | Participant token. Must belong to the authenticated site.       |
| `round`    | string | **Yes**  | Activity round. One of: `A1`, `A2`, `A3`, `A4`.                |
| `type`     | string | **Yes**  | Collection type. One of: `Blood`, `Urine`, `Mouthwash`.        |
| `status`   | string | **Yes**  | Collection status. One of: `Complete`, `Refused`.               |
| `date`     | string | No       | ISO 8601 timestamp of when the collection occurred.             |
| `location` | string | No       | Name of the facility or site where the collection took place.   |

#### Allowed Values

| Field    | Values                          |
|----------|---------------------------------|
| `round`  | `A1`, `A2`, `A3`, `A4`         |
| `type`   | `Blood`, `Urine`, `Mouthwash`  |
| `status` | `Complete`, `Refused`           |

#### Response

**`200 OK`** — All items processed successfully.  
**`206 Partial Content`** — Some items had errors.

```json
{
  "data": [
    {
      "Success": {
        "Token": "c3ed18b3-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        "Round": "A1",
        "Type": "Blood"
      }
    },
    {
      "Invalid Request": {
        "Token": "bad-token-value",
        "Errors": "Token does not exist or does not belong to your site."
      }
    }
  ],
  "code": 200
}
```

#### Response Items

Each element in the `data` array is one of:

| Key                | When                           | Fields                                |
|--------------------|--------------------------------|---------------------------------------|
| `Success`          | Item saved successfully        | `Token`, `Round`, `Type`              |
| `Invalid Request`  | Validation or lookup failed    | `Token`, `Errors` (description)       |
| `Server Error`     | Firestore write failed         | `Token`, `Errors` (description)       |

#### Error Responses

| Status | Condition                                     | Body                                                                          |
|--------|-----------------------------------------------|-------------------------------------------------------------------------------|
| `400`  | `data` missing, not an array, empty, or >499  | `{ "message": "Bad request. ...", "code": 400 }`                             |
| `401`  | Missing or invalid authorization              | `{ "message": "Authorization failed!", "code": 401 }`                        |
| `403`  | Called in stage or prod environment            | `{ "message": "API not available in this environment.", "code": 403 }`       |
| `405`  | Non-POST method used                          | `{ "message": "Only POST requests are accepted!", "code": 405 }`            |
| `500`  | Unhandled server error                        | `{ "message": "...", "code": 500 }`                                          |

#### Validation Errors (per item)

When an item fails validation, it appears in the response array with an `Errors` string describing all issues:

- `token not defined in data object.`
- `round is required.`
- `Invalid round "X". Must be one of: A1, A2, A3, A4.`
- `type is required.`
- `Invalid type "X". Must be one of: Blood, Urine, Mouthwash.`
- `status is required.`
- `Invalid status "X". Must be one of: Complete, Refused.`
- `date must be a valid ISO 8601 timestamp.`
- `Token does not exist or does not belong to your site.`

#### Example

```bash
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {
        "token": "c3ed18b3-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        "round": "A1",
        "type": "Blood",
        "status": "Complete",
        "date": "2026-04-09T10:30:00.000Z",
        "location": "HP Research Medical"
      }
    ]
  }' \
  "https://us-central1-nih-nci-dceg-connect-dev.cloudfunctions.net/followupCollections"
```

---

## Data Model

These APIs interact with two new Firestore collections that follow the restructured, row-based data model.

### `activities` Collection

Stores one document per participant per round, describing what follow-up activities the participant is eligible for.

| Field          | Type     | Description                                              |
|----------------|----------|----------------------------------------------------------|
| `connectId`    | number   | Participant's Connect ID                                 |
| `token`        | string   | Participant token                                        |
| `siteCode`     | string   | Healthcare provider site code                            |
| `round`        | string   | Round identifier (`A1`, `A2`, `A3`, `A4`)                |
| `activityType` | string   | Round category (`SDS` or `CED`)                          |
| `activities`   | string[] | List of activities for this round                        |
| `creationDate` | string   | ISO 8601 timestamp of record creation                    |

### `siteCollections` Collection

Stores one document per collection event submitted by a site.

| Field       | Type   | Description                                              |
|-------------|--------|----------------------------------------------------------|
| `connectId` | number | Participant's Connect ID                                 |
| `token`     | string | Participant token                                        |
| `siteCode`  | string | Submitting site's code (from auth)                       |
| `round`     | string | Round identifier (`A1`, `A2`, `A3`, `A4`)                |
| `type`      | string | Collection type (`Blood`, `Urine`, `Mouthwash`)          |
| `status`    | string | Collection status (`Complete`, `Refused`)                |
| `date`      | string | ISO 8601 timestamp of the collection (optional)          |
| `location`  | string | Facility name (optional)                                 |
| `createdAt` | string | ISO 8601 timestamp of when the record was saved          |

---

## Local Development

Run locally using the Functions Framework (requires `gcloud auth application-default login`):

```bash
# Terminal 1 — Eligibility API on port 8080
npm run demo:eligibility

# Terminal 2 — Collections API on port 8081
npm run demo:collections
```

Then test against `http://localhost:8080` and `http://localhost:8081` respectively.

---

## Limits

| Constraint                       | Value |
|----------------------------------|-------|
| Max items per collection request | 499   |
