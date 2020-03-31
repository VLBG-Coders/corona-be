# Cases By Country

### Description

Retrieve a cases timeline for a country.

## 1.1 CasesDailyModel

```
{
    "confirmed": <number>,
    "date": <string>,
    "deaths": <number>,
    "recovered": <number>
}
```

## 1.2 Get daily cases for a country

**Endpoint:** `/cases-by-country?country=austria`

**Method:** `GET`

**Filter Url Parameter:**

Parameter Name | Parameter Type | Example
-------------- | -------------- | -------
country | string | `?country=austria`

**Response:** `200`

```
{
    "country": <CountryModel>,
    "timeline": [
        <CasesDailyModel>
    ]
}
```
