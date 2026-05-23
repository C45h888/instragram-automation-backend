# ================================
# Instagram Automation Dashboard - Backend API
# Node.js + Express Server
# ================================

FROM node:18-alpine

# Set working directory
WORKDIR /app

# Install runtime dependencies for better npm performance
RUN apk add --no-cache \
    python3 \
    make \
    g++ \
    curl

# Copy package files
COPY package*.json ./

# Install production dependencies only
# Using npm ci for deterministic, faster builds
ENV NODE_ENV=production
RUN npm ci --only=production && npm cache clean --force

# Copy application code
COPY . .

# Create logs directory with proper permissions
RUN mkdir -p logs && chmod 755 logs

# Create a non-root user for security
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001 && \
    chown -R nodejs:nodejs /app

# Switch to non-root user
USER nodejs

# Expose port 3001 (Express server)
EXPOSE 3001

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3001/health || exit 1

# Start the application
# Using node directly instead of npm for better signal handling
CMD ["node", "server.js"]
