import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import { initMonitor } from './monitor/client';

// 先渲染 React（不阻塞——即使监控初始化失败，页面仍正常显示）
ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);

// 后台初始化监控（失败静默——不影响页面正常功能）
initMonitor().catch(() => {});
