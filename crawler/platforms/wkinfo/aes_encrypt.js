#!/usr/bin/env node
'use strict';
/**
 * Only job of this file: generate the AES-encrypted captcha parameters
 * (pointJson / captchaVerification), exactly matching what
 * assets/js/verify-slipping/ase.js does on the real site:
 *
 *   function aesEncrypt(word, keyWord) {
 *     let key = CryptoJS.enc.Utf8.parse(keyWord);
 *     let srcs = CryptoJS.enc.Utf8.parse(word);
 *     let encrypted = CryptoJS.AES.encrypt(srcs, key, {mode: ECB, padding: Pkcs7});
 *     return encrypted.toString();
 *   }
 *
 * i.e. AES-128-ECB, PKCS7 padding, key = raw UTF-8 bytes of secretKey, no IV.
 * Image recognition (gap position / click-word points) is intentionally NOT here --
 * that's done in Python (recognize.py, via ddddocr) and only the final plaintext
 * {"x":..,"y":..} (or the list of points) is passed in here to be encrypted.
 *
 * CLI contract: stdin = {"word": "<plaintext>", "key": "<secretKey>"} (JSON, one line)
 *               stdout = {"cipher": "<base64 ciphertext>"}
 */
const crypto = require('crypto');

function aesEncrypt(word, secretKey) {
  const key = Buffer.from(secretKey, 'utf8');
  const cipher = crypto.createCipheriv('aes-128-ecb', key, null);
  cipher.setAutoPadding(true); // PKCS7
  return Buffer.concat([cipher.update(word, 'utf8'), cipher.final()]).toString('base64');
}

function readStdin() {
  return new Promise((resolve, reject) => {
    let chunks = [];
    process.stdin.on('data', d => chunks.push(d));
    process.stdin.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    process.stdin.on('error', reject);
  });
}

(async () => {
  try {
    const { word, key } = JSON.parse(await readStdin());
    process.stdout.write(JSON.stringify({ cipher: aesEncrypt(word, key) }));
  } catch (e) {
    process.stderr.write(String(e && e.stack || e));
    process.exit(1);
  }
})();
