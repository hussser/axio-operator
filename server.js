require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const { google } = require('googleapis');

const app = express();
const PORT = process.env.PORT || 3000;

const AI_KEY = process.env.GROQ_API_KEY || process.env.ANTHROPIC_API_KEY || '';
const USE_GROQ = !!process.env.GROQ_API_KEY;
const GROQ_MODEL = 'llama-3.3-70b-versatile';

// ─── Gmail OAuth2 client ────────────────────────────────────────────────────
function getGmailClient() {
  const oauth2 = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI || `https://axio-blush.vercel.app/auth/google/callback`
  );
  if (process.env.GOOGLE_REFRESH_TOKEN) {
    oauth2.setCredentials({ refresh_token: process.env.GOOGLE_REFRESH_TOKEN });
  }
  return oauth2;
}

// ─── AI helpers ─────────────────────────────────────────────────────────────
async function streamChat(systemPrompt, messages, onChunk, onDone, onError) {
  if (USE_GROQ) {
    const resp = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${AI_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: GROQ_MODEL, max_tokens: 2048, stream: true,
        messages: [{ role: 'system', content: systemPrompt }, ...messages] })
    });
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const d = line.slice(6).trim();
        if (d === '[DONE]') { onDone(); return; }
        try { const j = JSON.parse(d); const t = j.choices?.[0]?.delta?.content; if (t) onChunk(t); } catch {}
      }
    }
    onDone();
  } else {
    const Anthropic = require('@anthropic-ai/sdk');
    const client = new Anthropic({ apiKey: AI_KEY });
    const stream = await client.messages.stream({ model: 'claude-sonnet-4-6', max_tokens: 2048, system: systemPrompt, messages });
    for await (const chunk of stream) {
      if (chunk.type === 'content_block_delta' && chunk.delta?.text) onChunk(chunk.delta.text);
    }
    onDone();
  }
}

async function callAI(systemPrompt, userMessage) {
  if (USE_GROQ) {
    const resp = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${AI_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: GROQ_MODEL, max_tokens: 1500,
        messages: [{ role: 'system', content: systemPrompt }, { role: 'user', content: userMessage }] })
    });
    const j = await resp.json();
    return j.choices?.[0]?.message?.content || '';
  } else {
    const Anthropic = require('@anthropic-ai/sdk');
    const client = new Anthropic({ apiKey: AI_KEY });
    const r = await client.messages.create({ model: 'claude-sonnet-4-6', max_tokens: 1500,
      messages: [{ role: 'user', content: userMessage }], system: systemPrompt });
    return r.content[0].text;
  }
}

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── System prompts ─────────────────────────────────────────────────────────
const AGENTS = {
  operator: {
    name: 'Axio Operator',
    prompt: `Tu es Axio, l'opérateur IA personnel d'un entrepreneur ambitieux.
Tu es son bras droit digital : efficace, précis, direct.
Tu parles toujours en français, tu vouvoies, tu vas droit au but.
Tu peux : rédiger des briefs de RDV, préparer des emails, créer des plans d'action,
analyser des marchés, préparer des pitchs, rédiger des propositions commerciales,
organiser les priorités de la semaine, répondre à toutes les demandes business.
Réponds toujours de façon structurée. Sois court et percutant.
Tu as accès à Gmail : tu peux envoyer des emails et lire/résumer la boîte mail.

ENVOI D'EMAIL : Quand l'utilisateur demande d'envoyer un email, génère le contenu puis ajoute EXACTEMENT cette ligne à la fin (rien d'autre après) :
AXIO_EMAIL:{"to":"destinataire@email.com","subject":"Objet de l'email","body":"Corps complet de l'email"}
Ne mets pas de markdown dans le body de l'email. Utilise \\n pour les sauts de ligne.

LECTURE EMAILS : Quand l'utilisateur demande de lire/résumer ses emails, réponds normalement avec le résumé qui t'a été fourni dans le contexte.`
  },
  brief: {
    name: 'Agent Brief',
    prompt: `Tu es l'agent Brief de Axio. Tu prépares des briefs de réunion ultra-complets.
Pour chaque réunion, tu fournis :
- Contexte & objectif de la réunion
- Points clés à aborder (3-5 max)
- Informations importantes sur l'interlocuteur/l'entreprise
- Questions stratégiques à poser
- Résultat attendu de la réunion
Sois concis, actionnable, professionnel.`
  },
  email: {
    name: 'Agent Email',
    prompt: `Tu es l'agent Email de Axio. Tu rédiges des emails professionnels percutants.`
  },
  plan: {
    name: 'Agent Plan',
    prompt: `Tu es l'agent Plan d'action de Axio. Tu structures les priorités et les plans.`
  },
  pitch: {
    name: 'Agent Pitch',
    prompt: `Tu es l'agent Pitch de Axio. Tu prépares des pitchs et argumentaires commerciaux.`
  }
};

// ─── Chat (SSE streaming) ────────────────────────────────────────────────────
app.post('/api/chat', async (req, res) => {
  const { messages, agentId = 'operator' } = req.body;

  if (!AI_KEY) {
    return res.status(400).json({ error: 'Clé API manquante' });
  }

  const agent = AGENTS[agentId] || AGENTS.operator;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  try {
    await streamChat(
      agent.prompt,
      messages,
      (text) => res.write(`data: ${JSON.stringify({ text })}\n\n`),
      () => { res.write('data: [DONE]\n\n'); res.end(); },
      (err) => { res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`); res.end(); }
    );
  } catch (err) {
    res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
    res.end();
  }
});

// ─── Gmail OAuth2 setup ──────────────────────────────────────────────────────
app.get('/auth/google', (req, res) => {
  if (!process.env.GOOGLE_CLIENT_ID || !process.env.GOOGLE_CLIENT_SECRET) {
    return res.send('Configurez GOOGLE_CLIENT_ID et GOOGLE_CLIENT_SECRET dans Vercel.');
  }
  const oauth2 = getGmailClient();
  const url = oauth2.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent',
    scope: [
      'https://www.googleapis.com/auth/gmail.send',
      'https://www.googleapis.com/auth/gmail.readonly',
      'https://www.googleapis.com/auth/gmail.modify'
    ]
  });
  res.redirect(url);
});

app.get('/auth/google/callback', async (req, res) => {
  const { code } = req.query;
  if (!code) return res.send('Erreur : pas de code');
  try {
    const oauth2 = getGmailClient();
    const { tokens } = await oauth2.getToken(code);
    res.send(`
      <html><body style="font-family:sans-serif;padding:40px;background:#0a0a0f;color:#fff">
        <h2 style="color:#8b5cf6">✅ Gmail connecté avec succès !</h2>
        <p>Ajoutez cette variable dans Vercel → Settings → Environment Variables :</p>
        <pre style="background:#1a1a2e;padding:16px;border-radius:8px;color:#a78bfa;word-break:break-all">
GOOGLE_REFRESH_TOKEN=${tokens.refresh_token || '⚠️ null — relancez /auth/google'}
        </pre>
        <p style="color:#888">Après avoir ajouté la variable, faites un Redeploy dans Vercel.</p>
      </body></html>
    `);
  } catch (err) {
    res.send('Erreur : ' + err.message);
  }
});

// ─── Gmail : lire les emails ─────────────────────────────────────────────────
app.get('/api/gmail/inbox', async (req, res) => {
  if (!process.env.GOOGLE_REFRESH_TOKEN) {
    return res.status(400).json({ error: 'Gmail non connecté. Allez sur /auth/google pour connecter.' });
  }
  try {
    const auth = getGmailClient();
    const gmail = google.gmail({ version: 'v1', auth });
    const maxResults = parseInt(req.query.max) || 10;

    const list = await gmail.users.messages.list({
      userId: 'me',
      maxResults,
      q: 'in:inbox -category:promotions -category:social'
    });

    if (!list.data.messages?.length) return res.json({ emails: [] });

    const emails = await Promise.all(
      list.data.messages.map(async (m) => {
        const msg = await gmail.users.messages.get({ userId: 'me', id: m.id, format: 'metadata',
          metadataHeaders: ['From', 'Subject', 'Date'] });
        const headers = msg.data.payload.headers;
        const get = (name) => headers.find(h => h.name === name)?.value || '';
        const snippet = msg.data.snippet || '';
        return {
          id: m.id,
          from: get('From'),
          subject: get('Subject'),
          date: get('Date'),
          snippet: snippet.substring(0, 200)
        };
      })
    );

    res.json({ emails });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Gmail : envoyer un email ────────────────────────────────────────────────
app.post('/api/send-email', async (req, res) => {
  const { to, subject, body } = req.body;
  if (!to || !subject || !body) return res.status(400).json({ error: 'Champs manquants' });

  if (!process.env.GOOGLE_REFRESH_TOKEN) {
    return res.status(400).json({
      error: 'Gmail non connecté. Connectez votre compte sur /auth/google'
    });
  }

  try {
    const auth = getGmailClient();
    const gmail = google.gmail({ version: 'v1', auth });

    const from = process.env.GMAIL_USER || 'me';
    const raw = Buffer.from(
      `From: Axio <${from}>\r\nTo: ${to}\r\nSubject: ${subject}\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n${body}`
    ).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

    await gmail.users.messages.send({ userId: 'me', requestBody: { raw } });
    res.json({ success: true, method: 'gmail' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Résumé IA des emails ────────────────────────────────────────────────────
app.post('/api/gmail/summarize', async (req, res) => {
  if (!process.env.GOOGLE_REFRESH_TOKEN) {
    return res.status(400).json({ error: 'Gmail non connecté.' });
  }
  try {
    const auth = getGmailClient();
    const gmail = google.gmail({ version: 'v1', auth });

    const list = await gmail.users.messages.list({ userId: 'me', maxResults: 15,
      q: 'in:inbox -category:promotions -category:social is:unread' });

    if (!list.data.messages?.length) {
      return res.json({ summary: 'Aucun email non lu dans votre boîte de réception.' });
    }

    const emails = await Promise.all(
      list.data.messages.map(async (m) => {
        const msg = await gmail.users.messages.get({ userId: 'me', id: m.id, format: 'metadata',
          metadataHeaders: ['From', 'Subject', 'Date'] });
        const headers = msg.data.payload.headers;
        const get = (name) => headers.find(h => h.name === name)?.value || '';
        return `De: ${get('From')} | Objet: ${get('Subject')} | ${msg.data.snippet?.substring(0,150)}`;
      })
    );

    const summary = await callAI(
      'Tu es Axio. Résume ces emails de façon concise et actionnable. Identifie ce qui est urgent. Réponds en français.',
      `Voici les emails non lus :\n\n${emails.join('\n\n')}`
    );

    res.json({ summary, count: emails.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Status ──────────────────────────────────────────────────────────────────
app.get('/api/status', (req, res) => {
  res.json({
    status: 'online',
    version: '2.0.0',
    agents: Object.keys(AGENTS),
    integrations: {
      ai: !!(process.env.GROQ_API_KEY || process.env.ANTHROPIC_API_KEY),
      gmail: !!process.env.GOOGLE_REFRESH_TOKEN,
      whatsapp: !!process.env.WHATSAPP_TOKEN,
      notion: !!process.env.NOTION_TOKEN
    }
  });
});

// ─── Webhook n8n entrant ─────────────────────────────────────────────────────
app.post('/webhook/n8n', async (req, res) => {
  const secret = req.headers['x-webhook-secret'];
  if (secret !== process.env.N8N_WEBHOOK_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  const { type, data } = req.body;
  try {
    let systemPrompt = AGENTS.operator.prompt;
    let userMessage = '';
    if (type === 'brief_rdv') {
      systemPrompt = AGENTS.brief.prompt;
      userMessage = `Prépare un brief complet pour ce RDV :\n- Titre : ${data.title}\n- Date/Heure : ${data.datetime}\n- Avec : ${data.attendees?.join(', ') || 'Non précisé'}\n- Description : ${data.description || 'Aucune'}\n- Contexte CRM : ${data.crm_context || 'Aucun'}`;
    } else if (type === 'daily_summary') {
      userMessage = `Résume ma journée de demain :\nRDVs : ${JSON.stringify(data.events)}\nTâches : ${JSON.stringify(data.tasks)}\nEmails : ${JSON.stringify(data.emails)}`;
    }
    const result = await callAI(systemPrompt, userMessage);
    res.json({ success: true, type, result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`\n🚀 Axio v2 démarré sur http://localhost:${PORT}`);
  console.log(`   AI: ${AI_KEY ? (USE_GROQ ? '✅ Groq' : '✅ Claude') : '❌ manquant'}`);
  console.log(`   Gmail: ${process.env.GOOGLE_REFRESH_TOKEN ? '✅ connecté' : '⚠️  /auth/google pour connecter'}\n`);
});
