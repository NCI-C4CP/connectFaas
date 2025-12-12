const { getResponseJSON, setHeaders, logIPAddress, getSecret, uspsUrl, delay, backoffMs, safeJSONParse, parseResponseJson } = require('./shared');
const { db } = require('./firestore');
const conceptIds = require('./fieldToConceptIdMapping');

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

const EARLY_EXPIRY_MS = 60_000;                     // Refresh 1 minute before expiry
let uspsTokenCache = { token: null, expiresAt: 0 }; // In-memory cache
let appSettingsDocRef = null;                       // Cached Firestore doc ref

/**
 * Pre-process the request body
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
        errors.push("Invalid or missing all fields: Bad Request");
        return { params, errors };
    }

    // Validate streetAddress (required)
    const streetAddress = payload.streetAddress?.trim();
    if (streetAddress) {
        params.streetAddress = streetAddress;
    } else {
        errors.push("streetAddress");
    }
    
    // Validate secondaryAddress (optional)
    const secondaryAddress = payload.secondaryAddress?.trim();
    if (secondaryAddress) {
        params.secondaryAddress = secondaryAddress;
    }

    // Validate city (required)
    const city = payload.city?.trim();
    if (city) {
        params.city = city;
    } else {
        errors.push("city");
    }
    
    // Validate state (required, 2-letter uppercase)
    const state = payload.state?.toString()?.trim()?.toUpperCase();
    if (state && /^[A-Z]{2}$/.test(state)) {
        params.state = state;
    } else {
        errors.push("state");
    }

    // Validate zipCode (required, 5 digits)
    const zip = payload.zipCode?.toString()?.trim();
    if (zip && /^\d{5}$/.test(zip)) {
        params.ZIPCode = zip;
    } else {
        errors.push("zipCode");
    }

    return { params, errors };
}

/**
 * Build the query string URL with the parameters
 * @param {Object} params - The parameters
 * @returns {string} - The query string URL with the parameters
 */
const buildURLWithParams = (params) => {
    const queryString = new URLSearchParams(params).toString();
    return `${uspsUrl.addresses}?${queryString}`;
}

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
 */
const persistToken = async (token, expiresInSeconds) => {
    if (!token || !expiresInSeconds) {
        console.error("USPS token: Invalid token or expiresInSeconds", { token, expiresInSeconds });
    }

    const computedLifetimeMs =
        typeof expiresInSeconds === "number" && expiresInSeconds > 0
            ? (expiresInSeconds * 1000) - EARLY_EXPIRY_MS
            : 5 * 60 * 1000; // Minimal 5m validity if missing (shouldn't happen)
            
    const expiresAt = Date.now() + computedLifetimeMs;

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

/**
 * Fetch a new USPS token from the API.
 * @param {boolean} forceRefresh - Ignore cache and force new fetch
 * @returns {Promise<string>} The access token
 */
const fetchUSPSToken = async (forceRefresh = false) => {
    const clientIdKey = process.env.USPS_CLIENT_ID;
    const clientSecretKey = process.env.USPS_CLIENT_SECRET;

    if (!clientIdKey || !clientSecretKey) {
        throw new Error("USPS credentials are not configured in environment variables.");
    }

    // Return cached token if valid and not forced
    if (!forceRefresh) {
        const cachedToken = await getCachedToken();
        if (cachedToken) return cachedToken;
    }

    const [clientId, clientSecret] = await Promise.all([
        getSecret(clientIdKey),
        getSecret(clientSecretKey),
    ]);

    const authorizedParams = new URLSearchParams({ 
        grant_type: "client_credentials", 
        client_id: clientId,
        client_secret: clientSecret,
        scope: "addresses" 
    });

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000); // 10s timeout for auth

    try {
        const authorizedResponse = await fetch(uspsUrl.auth, {
            method: "POST",
            headers: {
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            },
            body: authorizedParams,
            signal: controller.signal,
        });
        
        const body = await parseResponseJson(authorizedResponse);

        if (!authorizedResponse.ok) {
            throw new Error(
                `USPS auth failed (${authorizedResponse.status}): ${body?.error_description || body?.error || "unauthorized"}`
            );
        }

        if (!body?.access_token) {
            throw new Error("USPS auth succeeded but no access_token returned");
        }

        // 'expires_in' is in seconds (e.g., 28799)
        await persistToken(body.access_token, body.expires_in);
        
        return body.access_token;

    } catch (err) {
        throw new Error(`USPS auth network error: ${err.message}`);
    } finally {
        clearTimeout(timeout);
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
        return res.status(400).json(getResponseJSON("Bad Request", 400));
    }

    const { params, errors } = validateAddressParams(req.body);

    if (errors.length) {
        return res.status(400).json(getResponseJSON(`Invalid or missing fields: ${errors.join(", ")}`, 400));
    }

    const urlWithParams = buildURLWithParams(params);

    try {
        let token = await fetchUSPSToken(); // Try cache first
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
                        "Authorization": `Bearer ${token}`,
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

            // 401 Unauthorized: Token expired/revoked. Refresh and retry.
            if (uspsResponse.status === 401) {
                if (attempt < maxAttempts - 1) {
                    console.warn(`USPS address call unauthorized (attempt ${attempt + 1}). Refreshing token and retrying...`);
                    // Force refresh logic
                    try {
                        token = await fetchUSPSToken(true);
                    } catch (authErr) {
                         console.error("USPS token refresh failed during retry:", authErr);
                         return res.status(502).json(getResponseJSON("Address validation authentication failed", 502));
                    }
                    await delay(backoffMs(attempt));
                    continue;
                }
            }

            // Retry on throttling (429) or Server Errors (500+)
            if ((uspsResponse.status === 429 || uspsResponse.status >= 500) && attempt < maxAttempts - 1) {
                console.warn(`USPS address call ${uspsResponse.status} (attempt ${attempt + 1}); retrying...`);
                await delay(backoffMs(attempt));
                continue;
            }

            // Non-retryable error
            console.error("USPS address call failed (non-retryable):", {
                status: uspsResponse.status,
                error: responseBody?.error || responseBody?.message || "Unknown error"
            });
            return res.status(502).json(getResponseJSON("Address validation failed. Non-retryable error.", 502));
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
    buildURLWithParams,
};