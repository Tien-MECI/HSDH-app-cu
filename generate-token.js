import crypto from 'crypto';
import fs from 'fs';
import dotenv from 'dotenv';

dotenv.config();

// Tạo token mới
const newToken = crypto.randomBytes(32).toString('hex');

// Đọc file .env
const envPath = '.env';
let envContent = '';

if (fs.existsSync(envPath)) {
    envContent = fs.readFileSync(envPath, 'utf8');
}

// Cập nhật hoặc thêm token
const tokenLine = `WEBHOOK_AUTH_TOKEN=${newToken}`;
const lines = envContent.split('\n');
let found = false;

const updatedLines = lines.map(line => {
    if (line.startsWith('WEBHOOK_AUTH_TOKEN=')) {
        found = true;
        return tokenLine;
    }
    return line;
});

if (!found) {
    updatedLines.push(tokenLine);
}

// Ghi file
fs.writeFileSync(envPath, updatedLines.join('\n'));

console.log('✅ Token mới đã được tạo:');
console.log(`WEBHOOK_AUTH_TOKEN=${newToken}`);
console.log('\n⚠️  Lưu ý: Cần cập nhật lại token trong AppSheet!');