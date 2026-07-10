import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import { initMonitor } from './monitor/client';

// 初始化前端监控（不阻塞 React 渲染——即使配置获取失败，页面仍正常显示）
// 使用 .then() 而非 await，确保 initMonitor 内部的 catch 不会传播到渲染层
initMonitor().then(() => {
  ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  );
});
