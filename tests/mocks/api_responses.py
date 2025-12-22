"""
Mock utilities for external API responses (Telegram, Gemini).

This module provides helper functions for mocking HTTP responses
using the responses library.
"""

import json


def mock_telegram_success():
    """
    Returns mock data for a successful Telegram API response.
    
    Returns:
        dict: Response data matching Telegram API format
    """
    return {
        'ok': True,
        'result': {
            'message_id': 12345,
            'from': {
                'id': 987654321,
                'is_bot': True,
                'first_name': 'TestBot',
            },
            'chat': {
                'id': 789,
                'type': 'private',
                'username': 'testuser',
            },
            'date': 1703260800,
            'text': 'Test notification message',
        }
    }


def mock_telegram_error():
    """
    Returns mock data for a failed Telegram API response.
    
    Returns:
        dict: Error response data
    """
    return {
        'ok': False,
        'error_code': 400,
        'description': 'Bad Request: chat not found'
    }


def mock_gemini_success():
    """
    Returns mock data for a successful Gemini AI API response.
    
    Returns:
        dict: Response data matching Gemini API format
    """
    return {
        'candidates': [{
            'content': {
                'parts': [{
                    'text': '''**📊 Tổng quan thị trường:**

Thị trường chứng khoán Việt Nam có xu hướng tích cực trong phiên giao dịch hôm nay.

**📈 Cổ phiếu nổi bật:**

**HPG (Hòa Phát Group):**
- Giá hiện tại: 25,500 VND
- RSI: 45.2 (Trung lập)
- MACD: Tích cực
- Khuyến nghị: Mua

**VCB (Vietcombank):**
- Giá hiện tại: 95,000 VND
- RSI: 62.8 (Trung lập)
- MACD: Tích cực
- Khuyến nghị: Nắm giữ

**⚠️ Lưu ý:** Đây là phân tích tự động, không phải khuyến nghị đầu tư.'''
                }],
                'role': 'model'
            },
            'finishReason': 'STOP',
            'index': 0
        }],
        'usageMetadata': {
            'promptTokenCount': 150,
            'candidatesTokenCount': 200,
            'totalTokenCount': 350
        }
    }


def mock_gemini_error():
    """
    Returns mock data for a failed Gemini AI API response.
    
    Returns:
        dict: Error response data
    """
    return {
        'error': {
            'code': 400,
            'message': 'Invalid API key',
            'status': 'INVALID_ARGUMENT'
        }
    }


def get_telegram_url(token):
    """
    Constructs Telegram API URL.
    
    Args:
        token: Bot token
        
    Returns:
        str: Full API URL
    """
    return f'https://api.telegram.org/bot{token}/sendMessage'


def get_gemini_url():
    """
    Constructs Gemini API URL.
    
    Returns:
        str: Full API URL
    """
    return 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent'
