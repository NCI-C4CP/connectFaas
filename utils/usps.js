const { getResponseJSON, setHeaders, logIPAddress, getSecret, uspsUrl, delay, backoffMs, safeJSONParse, parseResponseJson } = require('./shared');
const { db } = require('./firestore');

/**
 * Address validation with the USPS API
 * 
 * Authentication:
 * Uses OAuth 2.0 "Client Credentials" flow.
 * - Access Token: Valid for ~8 hours (as per USPS docs).
 *   We request a new access token using the client_id/secret when the old one expires.
 * 
 * Caching Strategy:
 * 1. In-Memory Cache (`uspsTokenCache`): Works for warm Cloud Function instances only.
 * 2. Firestore (`appSettings` -> `usps`): For USPS API access token reuse across the 8-hour window.
 * 3. Fallback: Fetch new access token from USPS Auth API.
 */

const USPS_STATE_PATTERN = /^(AA|AE|AL|AK|AP|AS|AZ|AR|CA|CO|CT|DE|DC|FM|FL|GA|GU|HI|ID|IL|IN|IA|KS|KY|LA|ME|MH|MD|MA|MI|MN|MS|MO|MP|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PW|PA|PR|RI|SC|SD|TN|TX|UT|VT|VI|VA|WA|WV|WI|WY)$/;
const EARLY_EXPIRY_MS = 60_000;                     // Refresh 1 minute before expiry
let uspsTokenCache = { token: null, expiresAt: 0 }; // In-memory cache
let appSettingsDocRef = null;                       // Cached Firestore doc ref
let tokenRequestPromise = null;                     // Coalesces outbound USPS token requests

const normalizeAddressField = (value) => {
    if (value === undefined || value === null) return "";
    return value.toString().trim();
};

/**
 * Pre-process the request body
 * USPS address API requires streetAddress, state, and either city or ZIPCode.
 * @param {Object} payload - The request body
 * @returns {Object} - The validated parameters and errors
 */
const validateAddressParams = (payload) => {
    const errors = [];
    const params = {};

    if (typeof payload === "string") {
        payload = safeJSONParse(payload);
    }

    if (!payload || typeof payload !== "object") {
        errors.push("Invalid or missing all fields");
        return { params, errors };
    }

    const streetAddress = normalizeAddressField(payload.streetAddress);
    if (streetAddress) {
        params.streetAddress = streetAddress;
    } else {
        errors.push("missing streetAddress");
    }

    const secondaryAddress = normalizeAddressField(payload.secondaryAddress);
    if (secondaryAddress) {
        params.secondaryAddress = secondaryAddress;
    }

    const city = normalizeAddressField(payload.city);
    if (city) {
        params.city = city;
    }

    const state = normalizeAddressField(payload.state).toUpperCase();
    if (state && USPS_STATE_PATTERN.test(state)) {
        params.state = state;
    } else if (state) {
        errors.push("invalid state");
    } else {
        errors.push("missing state");
    }

    const zipCode = normalizeAddressField(payload.zipCode);
    if (zipCode && /^\d{5}$/.test(zipCode)) {
        params.ZIPCode = zipCode;
    }

    if (!params.city && !params.ZIPCode) {
        errors.push(zipCode ? "invalid zipCode" : "missing city or zipCode");
    }

    return { params, errors };
};

/**
 * Get the Firestore document reference AND update the local cache from Firestore.
 * If cached ref is available, uses it to get fresh snapshot.
 * If no cached ref, queries for it.
 * Updates uspsTokenCache global variable if valid token found in Firestore.
 */
const updateCacheFromFirestore = async () => {
    try {
        let snapshot;

        // If we already have the docRef, just fetch the fresh snapshot. Else, query for the document.
        if (appSettingsDocRef) {
            snapshot = await appSettingsDocRef.get();
        } else {
            const querySnapshot = await db
                .collection("appSettings")
                .where("appName", "==", "connectApp")
                .select("usps")
                .limit(1)
                .get();
            
            if (!querySnapshot.empty) {
                snapshot = querySnapshot.docs[0];
                appSettingsDocRef = snapshot.ref; // Update global ref
            }
        }

        if (snapshot && snapshot.exists) {
            const appSettingsData = snapshot.data();
            const uspsSettings = appSettingsData?.usps;
            const now = Date.now();
            
            if (uspsSettings?.token && uspsSettings?.expiresAt) {
                 if (uspsSettings.expiresAt - EARLY_EXPIRY_MS > now) {
                    // Update in-memory cache
                    uspsTokenCache = { 
                        token: uspsSettings.token, 
                        expiresAt: uspsSettings.expiresAt
                    };
                 }
            }

        } else if (!appSettingsDocRef) {
             console.error("USPS token: 'connectApp' settings doc not found in 'appSettings'. Firestore caching disabled.");
        }

    } catch (err) {
        console.error("USPS token: appSettings lookup failed", err);
    }
};

/**
 * Retrieve a valid token from caches (Memory -> Firestore).
 * @returns {Promise<string|null>} Token if found and valid, else null.
 */
const getCachedToken = async () => {
    const now = Date.now();
    
    // Check In-Memory Cache
    if (uspsTokenCache.token && uspsTokenCache.expiresAt - EARLY_EXPIRY_MS > now) {
        return uspsTokenCache.token;
    }

    // Check Firestore Cache (this will update the in-memory cache)
    await updateCacheFromFirestore();
    
    // Check In-Memory Cache Again (populated by updateCacheFromFirestore above)
    if (uspsTokenCache.token && uspsTokenCache.expiresAt - EARLY_EXPIRY_MS > now) {
        return uspsTokenCache.token;
    }

    return null;
};

/**
 * Persist the USPS token to caches.
 * Computes the expiration time based on the API response and stores it in the in-memory cache and Firestore.
 * @param {string} token - The access token
 * @param {number} expiresInSeconds - Expiration time in seconds from response
 * @param {number} issuedAt - Issued time in milliseconds from response
 * @return {Promise<string>} The token that was persisted
 */
const persistToken = async (token, expiresInSeconds, issuedAt) => {
    if (!token || !expiresInSeconds) {
        console.error("USPS token: Invalid token or expiresInSeconds", { hasToken: !!token, expiresInSeconds });
    }

    const computedLifetimeMs =
        typeof expiresInSeconds === "number" && expiresInSeconds > 0
            ? (expiresInSeconds * 1000)
            : 5 * 60 * 1000; // Minimal 5m validity if missing (shouldn't happen)
            
    let expiresAt;
    if (typeof issuedAt === "number" && issuedAt > 0) {
        expiresAt = issuedAt + computedLifetimeMs;
    } else {
        expiresAt = Date.now() + computedLifetimeMs;
    }

    uspsTokenCache = { token, expiresAt };

    // We need the docRef to write to Firestore. 
    if (!appSettingsDocRef) {
        await updateCacheFromFirestore();
    }

    if (appSettingsDocRef) {
        try {
            await appSettingsDocRef.set({ 
                usps: { token, expiresAt } 
            }, { merge: true });

        } catch (err) {
            console.warn("USPS token: Firestore write failed", err);
        }
    }
    return token;
};

const requestNewUSPSToken = async (clientId, clientSecret) => {
    const authorizationData = {
        grant_type: "client_credentials", 
        client_id: clientId,
        client_secret: clientSecret,
        scope: "addresses" 
    };

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000); // 10s timeout for auth

    try {
        const res = await fetch(uspsUrl.auth, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(authorizationData),
            signal: controller.signal,
        });
        
        const resJson = await res.json();
        if (!res.ok) {
            throw new Error(
                `USPS auth failed (${res.status}): ${resJson?.error_description || resJson?.error || "unauthorized"}`
            );
        }

        if (!resJson?.access_token) {
            throw new Error("USPS auth succeeded but no access_token returned");
        }

        // 'expires_in' is in seconds (e.g., 28799); 'issued_at' is Unix ms timestamp
        await persistToken(resJson.access_token, resJson.expires_in, resJson.issued_at);
        
        return resJson.access_token;
    } finally {
        clearTimeout(timeout);
    }
};

/**
 * Get a valid USPS token from cache or the API.
 *
 * Concurrent requests to USPS API are consolidated: once one caller starts a
 * USPS token request, other callers await the same tokenRequestPromise.
 *
 * @param {boolean} forceRefresh - Ignore cached tokens and request a new token.
 * @returns {Promise<string>} The access token.
 */
const getUSPSToken = async (forceRefresh = false) => {
    if (tokenRequestPromise) {
        return await tokenRequestPromise;
    }

    if (!forceRefresh) {
        const cachedToken = await getCachedToken();
        if (cachedToken) {
            return cachedToken;
        }
    }

    const clientIdKey = process.env.USPS_CLIENT_ID;
    const clientSecretKey = process.env.USPS_CLIENT_SECRET;
    if (!clientIdKey || !clientSecretKey) {
        throw new Error("USPS credentials are not configured in environment variables.");
    }

    const [clientId, clientSecret] = await Promise.all([
        getSecret(clientIdKey),
        getSecret(clientSecretKey),
    ]);

    if (!clientId || !clientSecret) {
        console.error("USPS credentials are not configured in environment variables.");
        return null;
    }

    let ownsTokenRequest = false;
    if (!tokenRequestPromise) {
        tokenRequestPromise = requestNewUSPSToken(clientId, clientSecret);
        ownsTokenRequest = true;
    }

    try {
        return await tokenRequestPromise;
    } finally {
        if (ownsTokenRequest) {
            tokenRequestPromise = null;
        }
    }
};

/**
 * Validate an address using the USPS API.
 * Endpoint handler.
 */
const addressValidation = async (req, res) => {
    logIPAddress(req);
    setHeaders(res);

    if (req.method !== "POST") {
        return res.status(405).json(getResponseJSON("Only POST requests are accepted!", 405));
    }

    if (!req.body) {
        console.error("USPS address validation: missing request body.");
        return res.status(400).json(getResponseJSON("Bad Request", 400));
    }

    const { params, errors } = validateAddressParams(req.body);

    if (errors.length) {
        console.error("USPS address validation: invalid fields.", { errors });
        return res.status(400).json(getResponseJSON(`Invalid or missing fields: ${errors.join(", ")}`, 400));
    }

    const queryString = new URLSearchParams(params).toString();
    const urlWithParams = `${uspsUrl.addresses}?${queryString}`;

    try {
        let accessToken = await getUSPSToken();
        const maxAttempts = 3;

        for (let attempt = 0; attempt < maxAttempts; attempt++) {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 10000); // 10s timeout per attempt
            
            let uspsResponse;
            try {
                uspsResponse = await fetch(urlWithParams, {
                    method: "GET",
                    headers: {
                        "Accept": "application/json",
                        "Authorization": `Bearer ${accessToken}`,
                    },
                    signal: controller.signal,
                });

            } catch (err) {
                clearTimeout(timeout);
                if (attempt < maxAttempts - 1) {
                    console.warn(`USPS address call network error (attempt ${attempt + 1}): ${err.message}. Retrying...`);
                    await delay(backoffMs(attempt));
                    continue;
                }
                console.error("USPS address call network error (final attempt):", err);
                return res.status(502).json(getResponseJSON("Address validation temporarily unavailable", 502));
            }
            clearTimeout(timeout);

            const responseBody = await parseResponseJson(uspsResponse);

            if (uspsResponse.ok) {
                return res.status(200).json(responseBody || {});
            }

            // 401 Unauthorized: token expired/revoked; refresh and retry.
            if (uspsResponse.status === 401) {
                if (attempt < maxAttempts - 1) {
                    console.warn(`USPS address call unauthorized (attempt ${attempt + 1}). Refreshing token and retrying...`);
                    // Force refresh token
                    try {
                        accessToken = await getUSPSToken(true);
                    } catch (authErr) {
                         console.error("USPS token refresh failed during retry:", authErr);
                         return res.status(502).json(getResponseJSON("Address validation authentication failed", 502));
                    }
                    await delay(backoffMs(attempt));
                    continue;
                }
            }

            // 429 Too Many Requests: USPS throttling;
            // 500+ Server Errors: USPS service issues;
            // retry with backoff if not the final attempt.
            if ((uspsResponse.status === 429 || uspsResponse.status >= 500) && attempt < maxAttempts - 1) {
                console.warn(`USPS address call ${uspsResponse.status} (attempt ${attempt + 1}); retrying...`);
                await delay(backoffMs(attempt));
                continue;
            }

            // Non-retryable error (USPS 4xx input issues) or USPS error code 10005 (Address Not Found - invalid/unknown address).
            const errorMessage =
                responseBody?.error?.message ||
                responseBody?.message ||
                responseBody?.error ||
                "Address validation failed. Non-retryable error.";
            console.error("USPS address call failed (non-retryable):", {
                status: uspsResponse.status,
                error: responseBody?.error || responseBody?.message || "Unknown error"
            });

            const statusCode = uspsResponse.status || 502;
            return res.status(statusCode).json(getResponseJSON(errorMessage, statusCode));
        }

        return res.status(502).json(getResponseJSON("Address validation temporarily unavailable. Exhausted retries.", 502));

    } catch (error) {
        console.error("Unexpected error at addressValidation:", error);
        return res.status(500).json(getResponseJSON("Internal Server Error", 500));
    }
};

module.exports = {
    addressValidation,
    validateAddressParams,
};
