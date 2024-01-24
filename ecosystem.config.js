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
    },
    restart_delay: 3000,
    max_restarts: 10,
    autorestart: true,
    max_memory_restart: '100M',
  }],

  deploy: { production: {} }
};
