# Configurar repositório no GitHub - Projetos-DataLake

O repositório Git local já está pronto (branch `main`, commit inicial feito).

## 1. Criar o repositório no GitHub

1. Acesse **https://github.com/new**
2. Em **Repository name** digite: **Projetos-DataLake**
3. Escolha **Public** (ou Private, se preferir)
4. **Não** marque "Add a README file" (o projeto já tem conteúdo)
5. Clique em **Create repository**

## 2. Conectar e enviar o projeto

No terminal, na pasta do projeto, execute (substitua `SEU_USUARIO` pelo seu usuário do GitHub):

```bash
cd /home/Projetos/Projetos-DataLake

# Adicionar o remote do GitHub
git remote add origin https://github.com/SEU_USUARIO/Projetos-DataLake.git

# Enviar o código (branch main)
git push -u origin main
```

Se o GitHub pedir autenticação, use um **Personal Access Token** como senha (em https://github.com/settings/tokens) ou configure SSH.

## 3. Usando GitHub CLI (opcional)

Se você instalar o `gh` e fizer login (`gh auth login`), pode criar o repositório e fazer o push de uma vez:

```bash
cd /home/Projetos/Projetos-DataLake
gh repo create Projetos-DataLake --public --source=. --remote=origin --push
```

---

**Resumo:** Repo local pronto → crie **Projetos-DataLake** em github.com/new → `git remote add origin https://github.com/SEU_USUARIO/Projetos-DataLake.git` → `git push -u origin main`.
