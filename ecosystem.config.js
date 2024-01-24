module.exports = {
  apps: [{
    name: 'py-miner',
    cwd: '.',
    script: './neurons/miner.py',
    watch: '.',
    interpreter: 'python3',
    log_date_format: 'YYYY-MM-DD HH:mm Z',
    env: {
      NODE_ENV: 'production',
      REDDIT_CLIENT_ID: "0wR8IuNvy6TU_nMO_S62Tw",
      REDDIT_CLIENT_SECRET: "rON6VGWWvZqhtc5eRgiGTye9HivqDQ",
    },
    restart_delay: 3000,
    max_restarts: 10,
    autorestart: true,
    max_memory_restart: '100M',
  }],

  deploy: { production: {} }
};
