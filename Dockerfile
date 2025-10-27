# Chọn image Node chính thức
FROM node:18

# Tạo thư mục làm việc
WORKDIR /app

# Copy file package.json và cài dependencies
COPY package*.json ./
RUN npm install --production

# Copy toàn bộ code
COPY . .

# Cổng mà app của bạn chạy
EXPOSE 8080

# Lệnh khởi chạy app
CMD ["npm", "start"]
