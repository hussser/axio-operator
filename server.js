require('dotenv').config();
const express = require('express');
const cors = require('cors');
const Anthropic = require('@anthropic-ai/sdk');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

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
Réponds toujours de façon structurée. Sois court et percutant.`
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

  if (!process.env.ANTHROPIC_API_KEY) {
    return res.status(400).json({ error: 'Clé API Anthropic manquante dans .env' });
  }

  const agent = AGENTS[agentId] || AGENTS.operator;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  try {
    const stream = await client.messages.stream({
      model: 'claude-sonnet-4-6',
      max_tokens: 2048,
      system: agent.prompt,
      messages
    });

    for await (const chunk of stream) {
      if (chunk.type === 'content_block_delta' && chunk.delta?.text) {
        res.write(`data: ${JSON.stringify({ text: chunk.delta.text })}\n\n`);
      }
    }

    res.write('data: [DONE]\n\n');
    res.end();

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

    const response = await client.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 1500,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }]
    });

    res.json({
      success: true,
      type,
      result: response.content[0].text
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

    const response = await client.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 500,
      system: AGENTS.operator.prompt + '\nTu réponds via WhatsApp : sois très concis (max 3 phrases). Pas de markdown.',
      messages: [{ role: 'user', content: userText }]
    });

    const replyText = response.content[0].text;

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
      claude: !!process.env.ANTHROPIC_API_KEY,
      google: !!process.env.GOOGLE_CLIENT_ID,
      whatsapp: !!process.env.WHATSAPP_TOKEN,
      notion: !!process.env.NOTION_TOKEN
    }
  });
});

app.listen(PORT, () => {
  console.log(`\n🚀 Axio AI Operator démarré sur http://localhost:${PORT}`);
  console.log(`   Claude: ${process.env.ANTHROPIC_API_KEY ? '✅ connecté' : '❌ clé manquante'}`);
  console.log(`   WhatsApp: ${process.env.WHATSAPP_TOKEN ? '✅ connecté' : '⚠️  non configuré'}`);
  console.log(`   Google: ${process.env.GOOGLE_CLIENT_ID ? '✅ connecté' : '⚠️  non configuré'}\n`);
});
