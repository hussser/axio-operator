require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const nodemailer = require('nodemailer');

const app = express();
const PORT = process.env.PORT || 3000;

const AI_KEY = process.env.GROQ_API_KEY || process.env.ANTHROPIC_API_KEY || '';
const USE_GROQ = !!process.env.GROQ_API_KEY;
const GROQ_MODEL = 'llama-3.3-70b-versatile';

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

// ─── System prompts par agent ───────────────────────────────────────────────
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

ENVOI D'EMAIL : Quand l'utilisateur demande d'envoyer un email, génère le contenu puis ajoute EXACTEMENT cette ligne à la fin (rien d'autre après) :
AXIO_EMAIL:{"to":"destinataire@email.com","subject":"Objet de l'email","body":"Corps complet de l'email"}
Ne mets pas de markdown dans le body de l'email. Utilise \\n pour les sauts de ligne.`
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
- Durée recommandée par point
Sois concis, actionnable, professionnel.`
  },
  email: {
    name: 'Agent Email',
    prompt: `Tu es l'agent Email de Axio. Tu rédiges des emails professionnels percutants.
Pour chaque email tu fournis :
- Objet (accrocheur, clair)
- Corps de l'email (structuré, professionnel, personnalisé)
- Call-to-action clair
Tu adaptes le ton selon le destinataire (client, partenaire, fournisseur, prospect).
Tu vas à l'essentiel. Pas de blabla inutile.`
  },
  plan: {
    name: 'Agent Plan',
    prompt: `Tu es l'agent Plan d'action de Axio. Tu structures les priorités et les plans.
Tu fournis toujours :
- Objectif principal (1 phrase)
- Actions prioritaires (avec deadlines)
- Ressources nécessaires
- Indicateurs de succès
- Risques et blocages potentiels
Format clair, structuré, avec des cases à cocher. Orienté résultats.`
  },
  pitch: {
    name: 'Agent Pitch',
    prompt: `Tu es l'agent Pitch de Axio. Tu prépares des pitchs et argumentaires commerciaux.
Tu structures selon le framework : Problème → Solution → Preuve → Offre → Action.
Tu adaptes le pitch selon la durée (30s, 2min, 5min) et l'audience (investisseur, client, partenaire).
Percutant, mémorable, orienté valeur.`
  }
};

// ─── Route chat principal (streaming SSE) ───────────────────────────────────
app.post('/api/chat', async (req, res) => {
  const { messages, agentId = 'operator' } = req.body;

  if (!AI_KEY) {
    return res.status(400).json({ error: 'Clé API manquante (GROQ_API_KEY ou ANTHROPIC_API_KEY)' });
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

// ─── Webhook entrant depuis n8n ─────────────────────────────────────────────
// n8n envoie des données ici après avoir collecté infos (agenda, CRM, etc.)
app.post('/webhook/n8n', async (req, res) => {
  const secret = req.headers['x-webhook-secret'];
  if (secret !== process.env.N8N_WEBHOOK_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { type, data } = req.body;

  try {
    let systemPrompt = AGENTS.operator.prompt;
    let userMessage = '';

    if (type === 'brief_rdv') {
      systemPrompt = AGENTS.brief.prompt;
      userMessage = `Prépare un brief complet pour ce RDV :
- Titre : ${data.title}
- Date/Heure : ${data.datetime}
- Avec : ${data.attendees?.join(', ') || 'Non précisé'}
- Description : ${data.description || 'Aucune'}
- Contexte CRM : ${data.crm_context || 'Aucun historique trouvé'}`;
    } else if (type === 'daily_summary') {
      userMessage = `Résume ma journée de demain et prépare-moi :
RDVs : ${JSON.stringify(data.events)}
Tâches en attente : ${JSON.stringify(data.tasks)}
Emails importants : ${JSON.stringify(data.emails)}`;
    } else if (type === 'email_reply') {
      systemPrompt = AGENTS.email.prompt;
      userMessage = `Rédige une réponse professionnelle à cet email :
De : ${data.from}
Objet : ${data.subject}
Message : ${data.body}
Instruction : ${data.instruction || 'Réponse professionnelle standard'}`;
    }

    const result = await callAI(systemPrompt, userMessage);

    res.json({
      success: true,
      type,
      result
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Webhook WhatsApp (Meta Business API) ──────────────────────────────────
// Vérification du webhook
app.get('/webhook/whatsapp', (req, res) => {
  if (req.query['hub.verify_token'] === process.env.WHATSAPP_VERIFY_TOKEN) {
    return res.send(req.query['hub.challenge']);
  }
  res.status(403).send('Forbidden');
});

// Réception des messages WhatsApp
app.post('/webhook/whatsapp', async (req, res) => {
  res.sendStatus(200);

  try {
    const entry = req.body.entry?.[0];
    const changes = entry?.changes?.[0];
    const message = changes?.value?.messages?.[0];
    if (!message || message.type !== 'text') return;

    const userText = message.text.body;
    const phoneNumber = message.from;

    const replyText = await callAI(
      AGENTS.operator.prompt + '\nTu réponds via WhatsApp : sois très concis (max 3 phrases). Pas de markdown.',
      userText
    );

    if (process.env.WHATSAPP_TOKEN && process.env.WHATSAPP_PHONE_ID) {
      await fetch(`https://graph.facebook.com/v17.0/${process.env.WHATSAPP_PHONE_ID}/messages`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${process.env.WHATSAPP_TOKEN}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          messaging_product: 'whatsapp',
          to: phoneNumber,
          type: 'text',
          text: { body: replyText }
        })
      });
    }
  } catch (err) {
    console.error('WhatsApp error:', err.message);
  }
});

// ─── Status endpoint ────────────────────────────────────────────────────────
app.get('/api/status', (req, res) => {
  res.json({
    status: 'online',
    version: '1.0.0',
    agents: Object.keys(AGENTS),
    integrations: {
      claude: !!process.env.GROQ_API_KEY || !!process.env.ANTHROPIC_API_KEY,
      google: !!process.env.GOOGLE_CLIENT_ID || !!process.env.N8N_WEBHOOK_SECRET,
      gmail: !!process.env.GMAIL_USER || !!process.env.N8N_WEBHOOK_SECRET,
      whatsapp: !!process.env.WHATSAPP_TOKEN,
      notion: !!process.env.NOTION_TOKEN
    }
  });
});

// ─── Envoi d'email via SMTP (Gmail App Password) ──────────────────────────
app.post('/api/send-email', async (req, res) => {
  const { to, subject, body } = req.body;
  if (!to || !subject || !body) return res.status(400).json({ error: 'Champs manquants' });

  // Via n8n si configuré
  if (process.env.N8N_SEND_EMAIL_URL) {
    try {
      await fetch(process.env.N8N_SEND_EMAIL_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-webhook-secret': process.env.N8N_WEBHOOK_SECRET || '' },
        body: JSON.stringify({ to, subject, body })
      });
      return res.json({ success: true, method: 'n8n' });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  // Via Gmail SMTP (App Password)
  if (process.env.GMAIL_USER && process.env.GMAIL_APP_PASSWORD) {
    try {
      const transporter = nodemailer.createTransport({
        service: 'gmail',
        auth: { user: process.env.GMAIL_USER, pass: process.env.GMAIL_APP_PASSWORD }
      });
      await transporter.sendMail({
        from: `"Axio" <${process.env.GMAIL_USER}>`,
        to, subject,
        text: body,
        html: body.replace(/\n/g, '<br>')
      });
      return res.json({ success: true, method: 'gmail' });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  res.status(400).json({ error: 'Email non configuré. Ajoutez GMAIL_USER et GMAIL_APP_PASSWORD dans les variables d\'environnement Vercel.' });
});

app.listen(PORT, () => {
  console.log(`\n🚀 Axio AI Operator démarré sur http://localhost:${PORT}`);
  console.log(`   Claude: ${process.env.ANTHROPIC_API_KEY ? '✅ connecté' : '❌ clé manquante'}`);
  console.log(`   WhatsApp: ${process.env.WHATSAPP_TOKEN ? '✅ connecté' : '⚠️  non configuré'}`);
  console.log(`   Google: ${process.env.GOOGLE_CLIENT_ID ? '✅ connecté' : '⚠️  non configuré'}\n`);
});
