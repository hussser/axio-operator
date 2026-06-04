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
    prompt: `Tu es Axio, l'opérateur IA personnel d'un entrepreneur ambitieux. Bras droit digital, efficace, direct.
Tu parles TOUJOURS en français. Tu vouvoies. Tu vas droit au but. Maximum 3 phrases sauf si on demande un document.

CAPACITÉS RÉELLES CONFIRMÉES — NE JAMAIS CONTREDIRE :
- Tu PEUX envoyer des emails. Infrastructure opérationnelle. Génère toujours AXIO_EMAIL.
- Tu PEUX lire/analyser Gmail. Le contexte email t'est fourni directement dans les messages.
- Ne dis JAMAIS "je ne peux pas", "je n'ai pas accès", "je ne suis pas connecté" concernant les emails. C'est faux.
- Si le contexte email est fourni dans le message, utilise-le pour répondre précisément.

ENVOI D'EMAIL — RÈGLE ABSOLUE :
Quand l'utilisateur demande d'envoyer un email (même en mode vocal), tu DOIS :
1. Rédiger le contenu de l'email
2. Ajouter OBLIGATOIREMENT cette ligne exacte à la toute fin de ta réponse :
AXIO_EMAIL:{"to":"adresse@email.com","subject":"Objet","body":"Corps de l'email. Utilise \\n pour les sauts de ligne."}
NE JAMAIS dire que tu ne peux pas envoyer - toujours générer le tag AXIO_EMAIL.

LECTURE EMAILS : Présente les emails par expéditeur, objet, et si c'est urgent. Sois précis et concis.`
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
  const { messages, agentId = 'operator', gmailContext } = req.body;

  if (!AI_KEY) {
    return res.status(400).json({ error: 'Clé API manquante' });
  }

  const agent = AGENTS[agentId] || AGENTS.operator;

  // Injecter les vrais emails dans le system prompt si disponibles
  const systemPrompt = gmailContext
    ? `${agent.prompt}\n\n=== BOÎTE MAIL RÉELLE (dernières 24h) ===\n${gmailContext}\n=== FIN EMAILS ===\nUtilise UNIQUEMENT ces données quand l'utilisateur parle d'emails. Ne jamais inventer d'emails.`
    : agent.prompt;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  try {
    await streamChat(
      systemPrompt,
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

// ─── Gmail : liste brute des emails (pour affichage UI) ─────────────────────
app.get('/api/gmail/inbox', async (req, res) => {
  const n8nUrl = process.env.N8N_READ_EMAIL_URL;
  if (!n8nUrl) return res.status(400).json({ emails: [] });
  try {
    const r = await fetch(n8nUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ maxResults: 20 })
    });
    const data = await r.json();
    const list = Array.isArray(data) ? data : [data];
    const since24h = Date.now() - 24 * 60 * 60 * 1000;
    const emails = list
      .filter(e => parseInt(e.internalDate || '0') > since24h)
      .map(e => ({
        id: e.id,
        from: e.From || e.from || '',
        subject: e.Subject || e.subject || '(sans objet)',
        snippet: (e.snippet || '').substring(0, 120),
        date: e.internalDate ? new Date(parseInt(e.internalDate)).toISOString() : ''
      }));
    res.json({ emails: emails.length > 0 ? emails : list.slice(0,10).map(e => ({
      from: e.From || e.from || '',
      subject: e.Subject || e.subject || '',
      snippet: (e.snippet || '').substring(0, 120),
      date: e.internalDate ? new Date(parseInt(e.internalDate)).toISOString() : ''
    }))});
  } catch (err) {
    res.status(500).json({ emails: [], error: err.message });
  }
});

// ─── Gmail : envoyer un email (via n8n) ────────────────────────────────────
app.post('/api/send-email', async (req, res) => {
  const { to, subject, body } = req.body;
  if (!to || !subject || !body) return res.status(400).json({ error: 'Champs manquants' });

  const n8nUrl = process.env.N8N_SEND_EMAIL_URL;
  if (!n8nUrl) return res.status(400).json({ error: 'Gmail non configuré.' });

  try {
    const r = await fetch(n8nUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to, subject, body })
    });
    const text = await r.text();
    let data;
    try { data = JSON.parse(text); } catch { data = {}; }
    if (!r.ok) return res.status(500).json({ error: data.message || `Erreur n8n ${r.status}` });
    res.json({ success: true, method: 'n8n-gmail' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Gmail : lire et résumer les emails (via n8n) ───────────────────────────
app.post('/api/gmail/summarize', async (req, res) => {
  const n8nUrl = process.env.N8N_READ_EMAIL_URL;
  if (!n8nUrl) return res.status(400).json({ error: 'Gmail non configuré.' });

  try {
    const r = await fetch(n8nUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ maxResults: 20 })
    });
    const emails = await r.json();

    if (!emails || (Array.isArray(emails) && emails.length === 0)) {
      return res.json({ summary: 'Aucun email non lu dans votre boîte de réception.', count: 0 });
    }

    const emailList = Array.isArray(emails) ? emails : [emails];

    // Filtrer les 24 dernières heures
    const since24h = Date.now() - 24 * 60 * 60 * 1000;
    const recent = emailList.filter(e => {
      const ts = parseInt(e.internalDate || '0');
      return ts > since24h;
    });
    const toProcess = recent.length > 0 ? recent : emailList.slice(0, 10);

    // Extraire expéditeur propre (ex: "Jean Dupont <jean@email.com>" → "Jean Dupont")
    const extractName = (from) => {
      const m = String(from).match(/^"?([^"<]+)"?\s*</);
      return m ? m[1].trim() : String(from).split('@')[0];
    };

    const lines = toProcess.map(e => {
      const from = extractName(e.From || e.from || '');
      const subject = e.Subject || e.subject || '(sans objet)';
      const snippet = String(e.snippet || '').substring(0, 120).replace(/\s+/g, ' ');
      const date = e.internalDate ? new Date(parseInt(e.internalDate)).toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' }) : '';
      return `• ${date ? date + ' — ' : ''}De ${from} : "${subject}" — ${snippet}`;
    });

    const period = recent.length > 0 ? 'des dernières 24h' : 'récents non lus';
    const summary = await callAI(
      `Tu es Axio. Présente ces emails ${period} de façon claire : liste chaque expéditeur et l'objet de son message. Mentionne ce qui semble urgent ou important. Sois concis. Réponds en français.`,
      `${toProcess.length} email(s) non lu(s) ${period} :\n\n${lines.join('\n')}`
    );

    res.json({ summary, count: toProcess.length, period });
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
      gmail: !!(process.env.N8N_SEND_EMAIL_URL || process.env.GOOGLE_REFRESH_TOKEN),
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
