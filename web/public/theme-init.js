(() => {
  try {
    const theme = localStorage.getItem('cclb.theme');
    if (theme === 'dark' || theme === 'light' || theme === 'system') {
      document.documentElement.setAttribute('data-theme', theme);
    }
  } catch {
    // Storage can be unavailable before the application has mounted.
  }
})();
