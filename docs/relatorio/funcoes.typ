// Aplica a formatação de Apêndice ao documento a partir deste ponto
#let apendices(doc) = {
  pagebreak()
  
  // Reseta o contador de títulos
  counter(heading).update(0)
  
  // Define que, a partir daqui, headings de nível 1 são tratados como "Apêndice"
  set heading(
    supplement: [Apêndice],
    numbering: "A"
  )
  
  // Estiliza a exibição do título no documento
  show heading.where(level: 1): it => {
    set text(weight: "bold")
    block(width: 100%)[
      APÊNDICE #counter(heading).display("A") – #it.body
    ]
  }
  
  doc
}

// Aplica a formatação de Anexo ao documento a partir deste ponto
#let anexos(doc) = {
  pagebreak()
  
  // Reseta o contador de títulos
  counter(heading).update(0)
  
  // Define que, a partir daqui, headings de nível 1 são tratados como "Anexo"
  set heading(
    supplement: [Anexo],
    numbering: "A"
  )
  
  // Estiliza a exibição do título no documento
  show heading.where(level: 1): it => {
    set text(weight: "bold")
    block(width: 100%)[
      ANEXO #counter(heading).display("A") – #it.body
    ]
  }
  
  doc
}