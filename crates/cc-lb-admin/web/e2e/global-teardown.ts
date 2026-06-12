export default async function globalTeardown() {
  if (process.env.MOCK_SERVER_PID) {
    try {
      process.kill(-Number(process.env.MOCK_SERVER_PID));
    } catch (e) {
      try {
        process.kill(Number(process.env.MOCK_SERVER_PID));
      } catch (e2) {
        // ignore
      }
    }
  }
}
