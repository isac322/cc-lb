async function verify() {
  import('vite').then(vite => {
    if (vite.rolldownVersion) {
      console.log(`Rolldown version: ${vite.rolldownVersion}`)
      process.exit(0)
    } else {
      console.error('Rolldown version not found on vite export')
      process.exit(1)
    }
  }).catch(err => {
    console.error(err)
    process.exit(1)
  })
}

verify()