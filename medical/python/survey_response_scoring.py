"""
Cloud Run function to analyze patient survey responses using Claude AI via OpenRouter.
Scores freeform text responses across 17 categories for sentiment analysis.
Now with batch processing support!
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import functions_framework
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import openai
from tenacity import retry, stop_after_attempt, wait_exponential
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = 'analytics-443821'
SOURCE_DATASET_ID = 'analytics'
TARGET_DATASET_ID = 'llm'
SOURCE_TABLE = f'{PROJECT_ID}.{SOURCE_DATASET_ID}.fct_intakes'
TARGET_TABLE = f'{PROJECT_ID}.{TARGET_DATASET_ID}.intake_freeform_scores'
OPENROUTER_API_KEY = os.environ.get('OPENROUTER_API_KEY')
OPENROUTER_URL = ""

MODEL = ""
PROVIDER = ""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"PROJECT_ID is set to: {PROJECT_ID}")
logger.info(f"TARGET_TABLE is set to: {TARGET_TABLE}")

# Batch configuration
API_BATCH_SIZE = 20  # Number of intakes to include in each batch request
MAX_PARALLEL_BATCHES = 4  # Number of concurrent batch requests

# Categories to score
EXPERIENCE_CATEGORIES = [
    'anxiety', 'diet', 'focus', 'energy', 'mood', 'mental_clarity',
    'sleep', 'weight_loss', 'skin', 'strength', 'sex_drive',
    'confidence', 'relaxation', 'erectile_function', 'hair_growth', 'exercise'
]

GENERAL_CATEGORY = ['general']
ALL_CATEGORIES = EXPERIENCE_CATEGORIES + GENERAL_CATEGORY

LLM_PROMPT = """You are a customer service representative, working for a medical company that specializes in prescribing medications for six main categories: Hair loss prevention, weight loss, testosterone, human growth hormone level increase, mood stress and sleep improvement, and sex drive/libido. Customers fill out a survey when they initially begin, along with a monthly ongoing survey, where they are given an opportunity to provide freeform text about their goals, and/or any side effects, benefits, or downsides associated with the medication they are taking. You are analyzing customer feedback.

Your task is to review their feedback and grade it in one, several, or all 17 categories. The feedback doesn’t have to be graded on all 17 categories — only on the categories that it mentions or is relevant to.
The experience-related categories are as follows:

- Anxiety
- Diet
- Focus
- Energy
- Mood
- Mental Clarity
- Sleep
- Weight Loss
- Skin
- Strength
- Sex Drive
- Confidence
- Relaxation
- Erectile Function
- Hair Growth
- Exercise

Per each experience-related category, if the freeform response is relevant to the category, it should receive one of three numeric grade values:
- -1 if the sentiment of the feedback is negative, implying that things pertaining to the category have gotten worse. For instance, in the diet category, if a customer is eating more and they are prescribed a weight loss medication, that is a negative experience.
- 0 if the sentiment of the feedback is neutral, implying that there has been little or no beneficial effect of the medication in the category. For instance, if a customer is prescribed a hair regrowth medication and hasn’t noticed any change in their hair, they would grade as neutral in the Hair Regrowth category. Feedback should also be marked as neutral if they saw improvement in the category at some point in time, but have since seen decline in that improvement.
- 1 if the sentiment of the feedback is positive, implying that things have gotten better in the respective category. For instance, if a customer is prescribed testosterone and is noticing that they can lift more and workout longer, they would grade as positive in the Exercise category.

Below are some examples of all possible scores of each experience-related category. 
It’s important to remember that these are only snippets. Spotting these alone doesn’t necessarily mean they belong in the category they are being shown as demonstrative of. The entire context of the sentence and paragraph must be considered to make that determination.
Also consider that these are customer-typed responses, so spelling and grammar errors will exist. Use your best judgement in determining what the customer is saying.

——————————————————————————————————
Examples of Each Experience-related Category:
——————————————————————————————————
————
Anxiety
————
- Negative
    - "I feel more anxious"
    - "My anxiety is through the roof"
    - "I had my first ever panic attack"
- Neutral
    - "I have yet to notice any measurable change since starting, but I get that we’re very early in the process."
    - "Anxiety hasn't changed since starting."
    - "Hoping as the process continues I feel some sort of relief from the low energy, sex drive, and anxiety I face daily"
- Positive
    - "slowly starting feeling benefits like reduced stress and anxiety"
    - "anxiety levels going down"
————
Diet
————
- Negative
    - "My diet has been poorer"
    - "The only possible side effects I have noticed are an increase in appetite"
    - "I also feel hungry pretty much all the time now"
- Neutral
    - "I still get hungry between meals but do notice not over eating"
- Positive
    - "The biggest improvement is reduction in "food noise"/cravings"

————
Focus
————
- Negative
    - "no focus"
    - "I am struggling with weight management and focus"
- Neutral
    - "As far as my goals, I haven’t gotten close and I’d like to have a little more energy and focus but there has been some improvement"
    - "I still have low energy, lack of drive and focus, plus ed issues"
- Positive
    - "Slight improvement in energy and focus"
    - "My focus at work and at home have improved"
————
Energy
————
- Negative
    - "I have a severe lack of energy"
    - "been feeling pretty energy exhausted"
    - "Low energy"
    - "I dont feel like my energy levels and focus are that good"
- Neutral
    - "First 2 days I felt very energized. Then, the following week, I felt lethargic"
- Positive
    - "Feeling more confident and have more energy"
————
Mood
————
- Negative
    - "I had one day where I felt a little moody and I skipped the next days pills and I was ok"
    - "My mood and brain fog improved for the first week but now I feel more easily agitated"
- Neutral
    - "Mood is still stable"
- Positive
    - "The most benefit I've felt so far was mood regulation when it comes to anxiety and depression. It drastically improved those things"
    - "energy levels are improved"
————
Mental Clarity
————
- Negative
    - "brain fog returned the 3rd week and I feel like I need to take naps again"
- Neutral
    - "no significant improvement in energy levels or mental clarity"
    - "I noticed the effects of the treatment right away, with more clarity and energy. Over time, though, the impact hasn’t felt as strong"
- Positive
    - "my mental clarity has improved"
    - "more mental clarity and less brain fog"
————
Sleep
————
- Negative
    - "sleepy around 5 pm"
    - "I’m sleeping worse"
    - "I’ve had a little trouble sleeping"
- Neutral
    - "I feel like energy, sleep, cognition, etc have improved.  I was hoping for more but perhaps it will take a bit longer"
- Positive
    - "Definitely feel better, sleep better and have more energy in just 2 weeks"
    - "I sleep like a champ"
————
Weight Loss
————
- Negative
    - "slight increase in abdominal fat"
    - "Although I’ve been able to improve strength and muscles mass, I still struggle reducing weight and belly fat"
- Neutral
    - "I’ve plateaued in my weight loss"
- Positive
    - "Ive lost 7 lbs in 2 weeks"
————
Skin
————
- Negative
    - "My only complaint is the back acne"
    - "Skin on my abdomen has been a little itchy"
    - "I’ve also had a breakout of acne around my eyes"
    - "The skin on my hands seems to dry and crack since taking king protocol"
- Neutral
    - "My skin hasn’t gotten much worse than before"
- Positive
    - "better skin"
    - "I have seen some more muscle tone and saw tightening of my skin throughout my body"
    - "Notice an improvement in skin quality"
————
Strength
————
- Negative
    - "Im lift less weight than when i started this product"
- Neutral
    - "There hasn’t been a noticeable increase in libido, erection strength, aggressiveness, or physical strength"
    - "My strength and muscle tone hasn't seen a noticeable shift"
    - "I think I have noticed some effects are far as minor improvements in strength, mood, sleep, and drive, but not sure if its a placebo effect"
- Positive
    - "I have not seen any additional benefit being on or off testosterone other than increased strength in gym"
    - "Noticed improvment in energy and strength about 14 days in"
    - "I am able to work out longer and seem to have more strength"
————
Sex Drive
————
- Negative
    - "also noticed libido decrease and not very strong erections and this was not an issue for me"
    - "My libido is still abysmal"
- Neutral
    - "Libido still moderate, erection about average"
    - "Slight improvement in libido, but not as much as anticipated"
- Positive
    - "better mood and sex life"
    - "My sex drive is up"
    - "Increased libido"
————
Confidence
————
- Negative
    - "I have been experiencing low energy levels, less confidence, essentially I’ve been acting like a little bitch"
- Neutral
    - "I also felt more calm and confident but this one also seems to be fading away"
- Positive
    - "Feeling more confident and have more energy"
    - "I think I've been feeling a lot more confident"
————
Relaxation
————
- Negative
    - "Sometimes hard to relax"
- Neutral
    - "I have changed quite some habits but still not feeling relaxed,  which I struggle with"
- Positive
    - "Overall I feel more calm, relaxed and confident"
    - "i seem to have more energy in the gym and generally feel more relaxed"
    - "I do feel more overall relaxed"
————
Erectile Function
————
- Negative
    - "also noticed libido decrease and not very strong erections and this was not an issue for me"
- Neutral
    - "I still am worried as I don’t get these strong erections and I used to get really good ones"
    - "My sex Drive has improved but erections still aren’t what they should be"
- Positive
    - "more morning erections"
    - "i have definitely have seen a positive difference in the ability to get get an erection, mostly in the morning"
————
Hair Regrowth
————
- Negative
    - "Have noticed more ERectile disfunction. And seemingly thinner hair"
    - "Hello Doc I hope this message finds you well, I’m not sure how much background context I am supposed to provide you with here, but I hope to convey that I have felt a marked decrease in multiple factors, that I am concerned has to do with lower end testosterone levels or at least non optimal for my body. These factors I mentioned are things like thinner hair, unexplained weight loss, libido, reduced strength, difficulty regulating emotions, unexplained fatigue"
- Neutral
    - "I think I have maintained/possibly a little bit more thinning for minoxidil shed. I am kind of interested if you think I will see any regrowth from the current regimen. I am in pretty late stage balding"
    - "Not seeing any noticeable improvement yet.  Feels like I’m not losing as much hair, but not seeing any noticeable regrowth"
- Positive
    - "Hair regrowth has been noticeable"
————
Exercise
————
- Negative
    - "still feel like I’m not recovering as well as possible between exercises"
- Neutral
    - "No gain in strength or endurance despite consistent resistance training and diet"
- Positive
    - "Been feeling more inclined to go out and exercise"
    - "capable of pushing harder in the gym"



In addition to the specific experience and plan categories, there is a general category that should ONLY be marked when the ENTIRE comment is general in nature, without references to any of the experience-related columns.
If the freeform response is general in nature, it should receive one of three numeric grade values:
    - -1 if the sentiment of the feedback is negative
    - 0 if the sentiment of the feedback is neutral
    - 1 if the sentiment of the feedback is positive

Examples of general responses for each possible score can be found below:
————
General
————

- Negative
    - "feeling overall much worse"
    - "everything has gotten worse"
    - "not happy with the results at all"
    - "feeling terrible overall"
- Neutral
    - "not noticing much of a difference"
    - "no changes yet"
    - "haven't noticed anything"
    - "too early to tell"
    - "nothing to report"
- Positive
    - "noticing significant improvements"
    - "feeling great overall"
    - "everything is better"
    - "very happy with the results"
    - "feeling amazing"

CRITICAL: If a portion of a response is graded in the General category, it can't also be graded in an experience-related category. For instance, if "Things are going great" is the entire response, its score would be a 1 and belong in the General category. However, if the entire response is, "Things are going great. Mood has been excellent", the response can be both graded in the General and Mood categories, since the customer's feedback about their mood is a separate segment of the feedback. However, if the response is, "Things like my mood have been great", the customer specifically mentioned positive feedback about their mood, so they should receive a 1 in the Mood category, but that same sentence should NOT be used to give them a 1 in the General category. Do your best to interpret what the customer is saying and determine which bucket the response deserves a grade in.
CRITICAL: Patients sometimes mention experiences of being prescribed the same medication that the survey response is for, but with a different provider than the one you, the customer representative, work for, which we are NOT interested in. Patients should NOT be graded on experiences from a different provider they were using in the past. They also sometimes report experiences that are the reason they are seeking medication, rather than an experienced effect of the medication. The respective experiences should NOT be graded in those cases, either. Therefore, it is important for you to consider whether the customer's survey response is coming from a first-time ("New") patient, which means they are submitting a survey response before being prescribed a medication, or an existing ("Refill") patient, which means they are submitting a survey response after being prescribed a medication by your company.

The protocol of the patient's intake should be considered in your review of the feedback. Here is PROTOCOL CONTEXT for better understanding:
- TESTOSTERONE Protocol typically affects: Strength, Sex Drive, Mood, Mental Clarity, Energy, Confidence
- HAIR REGROWTH Protocol typically affects: Hair Growth
- GROWTH HORMONE Protocol typically affects: Skin, Weight Loss, Mood, Energy, Sex Drive, Strength, Exercise, Sleep
- WEIGHT LOSS Protocol typically affects: Diet, Weight Loss
- BLOOD FLOW Protocol typically affects: Sex Drive, Exercise, Erectile Function
- MOOD, STRESS & SLEEP Protocol typically affects: Mood, Mental Clarity, Sleep, Focus, Relaxation, Energy

Use the protocol context above to better understand the patient's feedback. Categories outside of those highlighted above for each protocol can still be scored.

Read each response and rate the response, if applicable. Score it as -1 (negative), neutral (0), or positive (1), where applicable.

CRITICAL: Although most survey responses will be coherent, some are a jumbled mix of characters that can't be interpreted. For these instances, it is OK to mark all categories as null. It is also ok to mark all categories as null if there is no sentiment to be captured for any category.

For each response, return ONLY a JSON object with the following structure. Use null for categories that are not mentioned in the response:
{
    "uuid": "the intake uuid",
    "anxiety": -1/0/1/null,
    "diet": -1/0/1/null,
    "focus": -1/0/1/null,
    "energy": -1/0/1/null,
    "mood": -1/0/1/null,
    "mental_clarity": -1/0/1/null,
    "sleep": -1/0/1/null,
    "weight_loss": -1/0/1/null,
    "skin": -1/0/1/null,
    "strength": -1/0/1/null,
    "sex_drive": -1/0/1/null,
    "confidence": -1/0/1/null,
    "relaxation": -1/0/1/null,
    "erectile_function": -1/0/1/null,
    "hair_growth": -1/0/1/null,
    "exercise": -1/0/1/null,
    "general": -1/0/1/null
}

EXAMPLES OF PROPER SCORING:

Sample response: "I'm not looking any healthier which is one of my goals. Can I start taking a higher dose?"
Correct scoring: This mentions health as a goal but doesn't specify any particular category experiencing deterioration. Do NOT score any specific categories unless clearly mentioned. This statement alone does not justify a negative skin score or any other specific category score.

The scoring for this response should be:

{
    "uuid": "the intake uuid",
    "anxiety": null,
    "diet": null,
    "focus": null,
    "energy": null,
    "mood": null,
    "mental_clarity": null,
    "sleep": null,
    "weight_loss": null,
    "skin": null,
    "strength": null,
    "sex_drive": null,
    "confidence": null,
    "relaxation": null,
    "erectile_function": null,
    "hair_growth": null,
    "exercise": null,
    "general": 0
}

Sample response: "Hello, I recently was traveling and preparing for the holidays so my stress levels were higher than normal due to that. Also, since I was staying in a hotel and being awake at late hours to see family, my sleep quality was lower. I also went out for new years and partied so my energy and outlook on life was lower after that. I will say that I think my strength is going up overall, but hard to tell since I went to a different gym. I think my erections may be getting a bit stronger the last few days.  My weight and muscle went up to its highest since I started and may be due to holiday food and / or this medication. I dont see any side effects from the medication I would say for now. In Jan, I will have a more regular routine and can give a clearer answer on how this medication is doing for me. Let me know if you have questions."
Correct scoring: Since the patient expresses that his stress levels were higher due to travel and preparation for the holidays, we don't want to score him on mood or anxiety since it was related to his travel, not the medication. We also don't want to score him negatively on sleep, since he expressed that his sleep quality was lower as the result of staying in a hotel and staying up late. The patient also expressed that his outlook on life and energy were both low after going out for New Year's. We don't want to score him negatively on energy or mood for that, because he expressed that it was the result of going out.  However, he mentioned that his strength is going up overall, and that his erections may be getting stronger, so we want to score him positively on Strength and Sex Drive. He didn't specify any cause for those effects, so we assume it's because of the medication.

The scoring for this response should be:

{
    "uuid": "the intake uuid",
    "anxiety": null,
    "diet": null,
    "focus": null,
    "energy": null,
    "mood": null,
    "mental_clarity": null,
    "sleep": null,
    "weight_loss": null,
    "skin": null,
    "strength": 1,
    "sex_drive": 1,
    "confidence": null,
    "relaxation": null,
    "erectile_function": null,
    "hair_growth": null,
    "exercise": null,
    "general": null
}

Sample response from a new patient: "Evening! My name is Dawn. I turned 55 about a month ago. I feel like my diet really hasn’t changed much in the past few months but I continue to add weight. Started menopause. Please let me know what information I can provide."
Correct scoring: Because the patient is new, her neutral feedback on diet and negative feedback on Weight Loss should NOT be graded, as it is not related to any medication we have prescribed her.

The scoring for this response should be:

{
    "uuid": "the intake uuid",
    "anxiety": null,
    "diet": null,
    "focus": null,
    "energy": null,
    "mood": null,
    "mental_clarity": null,
    "sleep": null,
    "weight_loss": null,
    "skin": null,
    "strength": null,
    "sex_drive": null,
    "confidence": null,
    "relaxation": null,
    "erectile_function": null,
    "hair_growth": null,
    "exercise": null,
    "general": null
}

Sample response from a new patient: "Hi - I had successfully lost 40 lbs through Optavia and kept it off for 5+ years. My father passed away in June and my eating has been out of control. I keep trying to get back on track, but the food noise at night is terrible and I’ve been steadily gaining weight the last 3 months due to poor food choices with the food noise. I’m looking to stop the food noise so I can get back on track. I workout 2 times a week and walk 3-4 days a week. My goal is 10k steps a day. I eat very healthy, but have been making bad decisions and eating way too much in the evenings. I’m looking to micro dose a glp-1 to help me get back in control."
Correct scoring: Because the patient is new and lost all his/her weight through Optavia, a different medical provider, their negative feedback on diet and weight loss should NOT be graded.

The scoring for this response should be:

{
    "uuid": "the intake uuid",
    "anxiety": null,
    "diet": null,
    "focus": null,
    "energy": null,
    "mood": null,
    "mental_clarity": null,
    "sleep": null,
    "weight_loss": null,
    "skin": null,
    "strength": null,
    "sex_drive": null,
    "confidence": null,
    "relaxation": null,
    "erectile_function": null,
    "hair_growth": null,
    "exercise": null,
    "general": null
}
"""


class IntakeResponseAnalyzer:
    """Analyzes patient intake survey responses using Claude AI with batch processing."""
    
    def __init__(self):
        """Initialize the analyzer with BigQuery client and OpenRouter config."""
        # EXPLICITLY set the project when creating the client
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        self.openai_client = openai.OpenAI(
            base_url=OPENROUTER_URL,
            api_key=OPENROUTER_API_KEY,
        )
        self._ensure_target_table_exists()
    
    def _ensure_target_table_exists(self):
        """Create the target table if it doesnt exist."""
        table_id = TARGET_TABLE
        
        schema = [
            bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("goals_freeform", "STRING"),
            bigquery.SchemaField("protocol", "STRING"),
            bigquery.SchemaField("is_new", "BOOLEAN"),
            bigquery.SchemaField("scored_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("model_version", "STRING"),
            bigquery.SchemaField("is_unscorable", "BOOLEAN"),
        ]
        
        # Add score columns for each category
        for category in ALL_CATEGORIES:
            schema.append(bigquery.SchemaField(category, "INT64"))
        
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            self.bq_client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
        except NotFound:
            table = self.bq_client.create_table(table)
            logger.info(f"Created table {table_id}")
    
    def get_unscored_intakes(self, limit: int = 100) -> List[Dict]:
        """Fetch intakes that haven't been scored yet."""
        query = f"""
        SELECT 
            uuid
            , goals_freeform
            , protocol
            , COALESCE(new_refill = 'New', FALSE) AS is_new
        FROM {SOURCE_TABLE}
        WHERE COALESCE(goals_freeform, '') != ''
            AND uuid NOT IN (
                SELECT DISTINCT uuid
                FROM {TARGET_TABLE}
            )
            AND DATE(intake_completed_at) >= '2024-01-01'
        LIMIT {limit}
        """
        
        try:
            results = self.bq_client.query(query).result()
            intakes = [dict(row) for row in results]
            logger.info(f"Found {len(intakes)} unscored intakes")
            return intakes
        except Exception as e:
            logger.error(f"Error fetching unscored intakes: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def score_batch_with_claude(self, intakes: List[Dict]) -> List[Optional[Dict]]:
        """
        Send a batch of intakes to Claude for scoring via OpenRouter.
        
        Args:
            intakes: List of dictionaries with uuid, goals_freeform, protocol, is_new
            
        Returns:
            List of dictionaries with scores (same order as input)
        """
        try:
            # Build batch message
            batch_text = "Please analyze the following patient survey responses and score each one.\n\n"
            
            for idx, intake in enumerate(intakes, 1):
                batch_text += f"""
---RESPONSE {idx}---
UUID: {intake['uuid']}
Protocol: {intake['protocol']}
Is New Patient?: {intake['is_new']}
Response: {intake['goals_freeform']}

                """
            
            batch_text += "\nReturn ONLY the JSON objects, one per line, in the same order as the responses above, without any other text or formatting."
            
            response = self.openai_client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": LLM_PROMPT},
                    {"role": "user", "content": batch_text}
                ],
                temperature=0.1,
                max_tokens=10000,
            )
            
            logger.info(f"API Response - finish_reason: {response.choices[0].finish_reason}")
            logger.info(f"API Response - tokens used: prompt={response.usage.prompt_tokens}, completion={response.usage.completion_tokens}")
            
            content = response.choices[0].message.content
            
            # Check for empty or suspiciously short responses
            if not content or not content.strip() or response.usage.completion_tokens < 1000:
                logger.warning(f"Suspiciously short response from LLM (tokens={response.usage.completion_tokens}), retrying...")
                raise Exception(f"Empty or incomplete response from LLM API")
            
            logger.info(f"Raw response from LLM:\n{content}")
            
            # Parse multiple JSON objects
            scores_list = self._parse_batch_response(content, intakes)

            if len(scores_list) != len(intakes):
                logger.warning(f"Incomplete batch: got {len(scores_list)}/{len(intakes)} scores, retrying...")
                raise Exception(f"Incomplete batch response: expected {len(intakes)} scores but got {len(scores_list)}")
            
            logger.info(f"Successfully scored batch of {len(scores_list)} intakes")
            return scores_list
            
        except Exception as e:
            logger.error(f"Error scoring batch: {e}")
            raise
    
    def _parse_batch_response(self, content: str, intakes: List[Dict]) -> List[Dict]:
        """Parse batch response containing multiple JSON objects."""
        import re
        import json
        
        # Remove markdown code blocks if present
        content = re.sub(r'```json\s*', '', content)
        content = re.sub(r'```\s*', '', content)
        
        # Use dict instead of list to handle duplicates inline
        scores_by_uuid = {}
        
        # Strategy 1: Try line-by-line parsing (most reliable for batch responses)
        lines = content.strip().split('\n')
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line or not line.startswith('{'):
                continue
                
            try:
                scores = json.loads(line)
                
                if not isinstance(scores, dict):
                    logger.warning(f"Line {line_num}: Parsed JSON is not a dict: {type(scores)}")
                    continue
                
                # Find the matching intake by UUID
                matching_intake = next((i for i in intakes if i['uuid'] == scores.get('uuid')), None)
                
                if matching_intake:
                    # Check if all scores are null
                    all_null = all(scores.get(cat) is None for cat in ALL_CATEGORIES)
                    scores['is_unscorable'] = all_null
                    
                    # Add metadata
                    scores['goals_freeform'] = matching_intake['goals_freeform']
                    scores['protocol'] = matching_intake['protocol']
                    scores['is_new'] = matching_intake['is_new']
                    scores['scored_at'] = datetime.now(timezone.utc).isoformat()
                    scores['model_version'] = MODEL
                    
                    uuid = scores['uuid']
                    if uuid in scores_by_uuid:
                        logger.warning(f"Duplicate UUID in LLM response, overwriting with later scoring: {uuid}")
                    scores_by_uuid[uuid] = scores
                    
                    logger.info(f"Successfully parsed score for UUID: {uuid}")
                else:
                    logger.warning(f"No matching intake found for UUID: {scores.get('uuid')}")
                    
            except json.JSONDecodeError as e:
                logger.debug(f"Line {line_num} is not valid JSON: {e}")
                continue
        
        # Strategy 2: If line-by-line failed, try to find JSON objects with regex
        if len(scores_by_uuid) == 0:
            logger.info("Line-by-line parsing failed, trying regex approach")
            
            json_pattern = r'\{(?:[^{}]|(?:\{[^{}]*\}))*\}'
            matches = re.finditer(json_pattern, content, re.DOTALL)
            
            for match in matches:
                try:
                    scores = json.loads(match.group())
                    
                    if not isinstance(scores, dict):
                        continue
                    
                    matching_intake = next((i for i in intakes if i['uuid'] == scores.get('uuid')), None)
                    
                    if matching_intake:
                        all_null = all(scores.get(cat) is None for cat in ALL_CATEGORIES)
                        scores['is_unscorable'] = all_null
                        scores['goals_freeform'] = matching_intake['goals_freeform']
                        scores['protocol'] = matching_intake['protocol']
                        scores['is_new'] = matching_intake['is_new']
                        scores['scored_at'] = datetime.now(timezone.utc).isoformat()
                        scores['model_version'] = MODEL
                        
                        uuid = scores['uuid']
                        if uuid in scores_by_uuid:
                            logger.warning(f"Duplicate UUID in LLM response, overwriting with later scoring: {uuid}")
                        scores_by_uuid[uuid] = scores
                        
                except json.JSONDecodeError:
                    continue
        
        logger.info(f"Parsed {len(scores_by_uuid)} unique score objects from response (expected {len(intakes)})")
        
        return list(scores_by_uuid.values())
    
    def _create_unscorable_result(self, intake: Dict) -> Dict:
        """Create a result marked as unscorable."""
        scores = {'uuid': intake['uuid']}
        for category in ALL_CATEGORIES:
            scores[category] = None
        scores['is_unscorable'] = True
        scores['goals_freeform'] = intake['goals_freeform']
        scores['protocol'] = intake['protocol']
        scores['is_new'] = intake['is_new']
        scores['scored_at'] = datetime.now(timezone.utc).isoformat()
        scores['model_version'] = MODEL
        return scores
    
    def batch_score_intakes(self, intakes: List[Dict]) -> Dict[str, Any]:
        """
        Score multiple intakes using batch processing and write immediately.
        
        Args:
            intakes: List of intake dictionaries
            
        Returns:
            Dictionary with success/failure counts
        """
        total_processed = 0
        total_written = 0
        total_failed = 0
        
        # Split into batches
        for i in range(0, len(intakes), API_BATCH_SIZE):
            batch = intakes[i:i + API_BATCH_SIZE]
            batch_num = i//API_BATCH_SIZE + 1
            logger.info(f"Processing batch {batch_num}: {len(batch)} intakes")
            
            try:
                # Score the batch
                batch_scores = self.score_batch_with_claude(batch)
                total_processed += len(batch_scores)
                
                # Write immediately to BigQuery
                if batch_scores:
                    write_success = self.write_scores_to_bigquery(batch_scores)
                    if write_success:
                        total_written += len(batch_scores)
                        logger.info(f"Batch {batch_num}: Successfully wrote {len(batch_scores)} scores to BigQuery")
                    else:
                        total_failed += len(batch_scores)
                        logger.error(f"Batch {batch_num}: Failed to write scores to BigQuery")
                
                # Small delay between batches to avoid rate limits
                if i + API_BATCH_SIZE < len(intakes):
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Failed to score batch {batch_num} starting at index {i}: {e}")
                # DO NOT create unscorable results for failed batches
                # Let them remain unscored so they can be retried later
                total_failed += len(batch)
                continue
        
        return {
            'total_processed': total_processed,
            'total_written': total_written,
            'total_failed': total_failed
        }

    def write_scores_to_bigquery(self, scores: List[Dict]) -> bool:
        """Write scored results to BigQuery."""
        if not scores:
            logger.info("No scores to write")
            return True
        
        # Retry logic for BigQuery writes (handles auth refresh issues)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                rows_to_insert = []
                for score in scores:
                    if not isinstance(score, dict):
                        logger.error(f"Invalid score object type: {type(score)}")
                        continue
                    
                    if 'uuid' not in score:
                        logger.error(f"Score object missing uuid: {score}")
                        continue
                        
                    row = {
                        'uuid': score['uuid'],
                        'goals_freeform': score.get('goals_freeform'),
                        'protocol': score.get('protocol'),
                        'is_new': score.get('is_new'),
                        'scored_at': score.get('scored_at'),
                        'model_version': score.get('model_version'),
                        'is_unscorable': score.get('is_unscorable', False)
                    }
                    
                    for category in ALL_CATEGORIES:
                        row[category] = score.get(category)
                    
                    rows_to_insert.append(row)
                
                if not rows_to_insert:
                    logger.error("No valid rows to insert after filtering")
                    return False
                
                # Recreate client on retry to force credential refresh
                if attempt > 0:
                    logger.info(f"Retry {attempt}: Recreating BigQuery client")
                    bq_client = bigquery.Client(project=PROJECT_ID)
                else:
                    bq_client = self.bq_client
                
                table_id = TARGET_TABLE
                logger.info(f"Attempting to insert {len(rows_to_insert)} rows into {table_id}")
                
                table_ref = bq_client.get_table(table_id)
                logger.info(f"Table reference confirmed: {table_ref.full_table_id}")
                
                errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
                
                if errors:
                    logger.error(f"Failed to insert rows: {errors}")
                    return False
                
                logger.info(f"Successfully inserted {len(rows_to_insert)} rows to {table_id}")
                return True
                
            except TypeError as e:
                if "string indices must be integers" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"BigQuery auth refresh error (attempt {attempt + 1}/{max_retries}), retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
                    continue
                else:
                    logger.error(f"Error writing to BigQuery: {e}")
                    logger.exception("Full traceback:")
                    return False
            except Exception as e:
                logger.error(f"Error writing to BigQuery: {e}")
                logger.exception("Full traceback:")
                return False
        
        return False
        
    def run(self, total_records: int = 100) -> Dict[str, Any]:
        """Main execution flow."""
        start_time = datetime.now(timezone.utc)
        
        try:
            intakes = self.get_unscored_intakes(total_records)
            
            if not intakes:
                return {
                    'status': 'success',
                    'message': 'No unscored intakes found',
                    'processed': 0,
                    'written': 0,
                    'failed': 0,
                    'duration': (datetime.now(timezone.utc) - start_time).total_seconds()
                }
            
            # Process and write incrementally
            results = self.batch_score_intakes(intakes)
            
            status = 'success'
            if results['total_failed'] > 0:
                status = 'partial' if results['total_written'] > 0 else 'failed'
            
            return {
                'status': status,
                'message': f"Processed {results['total_processed']}/{len(intakes)} intakes, wrote {results['total_written']}, failed {results['total_failed']}",
                'processed': results['total_processed'],
                'written': results['total_written'],
                'failed': results['total_failed'],
                'total': len(intakes),
                'duration': (datetime.now(timezone.utc) - start_time).total_seconds()
            }
            
        except Exception as e:
            logger.error(f"Error in main execution: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'processed': 0,
                'written': 0,
                'failed': 0,
                'duration': (datetime.now(timezone.utc) - start_time).total_seconds()
            }


if __name__ == "__main__":
    # Get total_records from environment variable or default to 60000
    total_records = int(os.environ.get('TOTAL_RECORDS', '60000'))
    
    if not OPENROUTER_API_KEY:
        logger.error('OPENROUTER_API_KEY environment variable not set')
        exit(1)
    
    analyzer = IntakeResponseAnalyzer()
    result = analyzer.run(total_records=total_records)
    print(json.dumps(result, indent=2))
    
    # Exit with error code if failed
    if result['status'] == 'error':
        exit(1)
